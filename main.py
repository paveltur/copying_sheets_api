from authentication import ApiGoogle
import datetime as dt
import smtplib
import json

column_a = "A2:A"
columns_ad = "A2:D"
columns_cd = "C2:D"
column_m = "M2:M"
date_column = "G2:G"
range_for_copy = "A2:U"
range_for_copy_abandoned = "A2:AB"
DAYS = 5

with open("creds_sheets.json", "r") as f:
    creds_sheets = json.load(fp=f)
CALLS = creds_sheets["phone_calls"]
HISTORY = creds_sheets["HISTORY"]
MY_EMAIL = creds_sheets["mail_creds"]["MY_EMAIL"]
PASSWORD = creds_sheets["mail_creds"]["PASSWORD"]
TO_EMAILS = creds_sheets["mail_creds"]["TO_EMAILS"]


def copying_main_data(name: str,
                      call_list: str,
                      history_data: str,
                      history_sheet_id: str,
                      history_sheet_name: str):
    global range_for_copy
    if name == "abandoned":
        range_for_copy = range_for_copy_abandoned

    num_rows = len(ApiGoogle(call_list, f"{name}!{columns_cd}").read_data_ranges()["valueRanges"][0]["values"])
    # получаем количество строк на листе обзвона
    if name == "pdl_old" or name == "il":
        ApiGoogle(call_list, f"{name}!{column_a}").update_data({"values": [[f'{name}']]*num_rows})
    # для илов и пдл_олд добавляем имя обзвона в столбец А

    data_1 = ApiGoogle(history_data, f"{history_sheet_name}!{columns_ad}").read_data_ranges()

    data_2 = ApiGoogle(call_list, [f"{name}!{range_for_copy}{num_rows + 1}"]).read_data_ranges()
    # забираем весь диапазон листа обзвона для копирования (прим. листы периодически очищаются для быстроты загрузки,
    # поэтому забираем диапазон полностью)

    try:  # делаем список для поиска последнего исторического значения
        list_for_last_hist_values = data_1["valueRanges"][0]["values"]
    except KeyError:
        list_for_last_hist_values = []

    if name == "s7d":
        check_column = 2
        list_of_rows = []
        for row in data_2["valueRanges"][0]["values"]:
            try:
                if row[0] == "step 7" or row[0] == "decision":
                    list_of_rows.append(row)
            except IndexError:
                continue
        try:
            last_hist_value = [el for el in list_for_last_hist_values if el[0] == "step 7" or el[0] == "decision"][-1]
        except IndexError:
            last_hist_value = None

    elif name == "step 2-3":
        check_column = 2
        list_of_rows = []
        if history_sheet_name == "Q4_22":
            for row in data_2["valueRanges"][0]["values"]:
                try:
                    if row[0] == "step 2-3" and row[12]:
                        list_of_rows.append(row)
                except IndexError:
                    continue
        else:
            for row in data_2["valueRanges"][0]["values"]:
                try:
                    if row[0] == "step 2-3":
                        list_of_rows.append(row)
                except IndexError:
                    continue

        try:
            last_hist_value = [el for el in list_for_last_hist_values if el[0] == f"{name}"][-1]
        except IndexError:
            last_hist_value = None

    elif name == "abandoned":
        check_column = 0
        list_of_rows = data_2["valueRanges"][0]["values"]
        try:
            last_hist_value = list_for_last_hist_values[-1]
        except KeyError:
            last_hist_value = None

    else:
        check_column = 3
        list_of_rows = [row for row in data_2["valueRanges"][0]["values"] if row[0] == f"{name}"]
        try:
            last_hist_value = [el for el in list_for_last_hist_values if el[0] == f"{name}"][-1]
        except IndexError:
            last_hist_value = None

    if last_hist_value:
        try:
            index_cut_list = int([list_of_rows.index(row) for row in list_of_rows
                                  if row[check_column] == last_hist_value[check_column]][0]) + 1
        except IndexError:
            index_cut_list = None
    else:
        index_cut_list = None

    if name == "il":
        last_called_index = len(ApiGoogle(call_list, [f"{name}!{column_m}"]).read_data_ranges()
                                ["valueRanges"][0]["values"])
    else:
        last_called_index = None

    range_body = {"values": list_of_rows[index_cut_list:last_called_index]}
    try:
        last_row_history_list = len(list_for_last_hist_values) + 2
    except KeyError:
        last_row_history_list = 2

    range_for_writing = f"{history_sheet_name}!A{last_row_history_list}:{range_for_copy.split(':')[-1]}"
    if not range_body["values"]:
        return {"name": name, "func": "copied", "result": "true_2", "info": "no data to copy"}
    else:
        ApiGoogle(history_data).add_1_row_end(history_sheet_id)
        add_new_data = ApiGoogle(history_data, range_for_writing).update_data(range_body)
        return {"name": name, "func": "copied", "result": "true", "info": add_new_data}


def add_statuses_as_formula(data: dict):
    sheet_name = data["info"]["updatedRange"].split("!")[0]
    list_cells = data["info"]["updatedRange"].split("!")[1].split(":")
    nums_cells = []
    for el in list_cells:
        new_el = "".join(char for char in el if char.isdigit())
        nums_cells.append(int(new_el))
    formulas_h_column = []
    formulas_i_column = []
    if data["name"] == "s7d" or data["name"] == "step 2-3":
        for num_row in range(nums_cells[0], nums_cells[1] + 1):
            formulas_h_column.append([f'=ЕСЛИОШИБКА(ВПР(C{num_row};draft_auto!$A:$C;2;0);"")'])
            formulas_i_column.append([f'=ЕСЛИОШИБКА(ВПР(C{num_row};draft_auto!$A:$C;3;0);"")'])
    else:
        for num_row in range(nums_cells[0], nums_cells[1] + 1):
            formulas_h_column.append([f'=ЕСЛИОШИБКА(ВПР(D{num_row};draft_auto!$E:$G;2;0);"")'])
            formulas_i_column.append([f'=ЕСЛИОШИБКА(ВПР(D{num_row};draft_auto!$E:$G;3;0);"")'])

    info_for_writing = [{"range_for_writing": f"{sheet_name}!H{nums_cells[0]}:H{nums_cells[1]}",
                         "body": {"values": formulas_h_column}},
                        {"range_for_writing": f"{sheet_name}!I{nums_cells[0]}:I{nums_cells[1]}",
                         "body": {"values": formulas_i_column}}]
    result = []
    for column in info_for_writing:
        statuses_results = ApiGoogle(data["info"]["spreadsheetId"],
                                     column["range_for_writing"]).update_data(column["body"])
        result.append({"name": data["name"], "func": "added",
                       "result": "true", "info": statuses_results})
    return result


def check_backup_statuses(history_data, history_sheet_name, start_row, end_row, date_backup_statuses):
    data_stat_value = ApiGoogle(history_data, [f"{history_sheet_name}!A{start_row + 1}:I{end_row}"]).read_data_ranges()
    calls_info = []
    if history_sheet_name == "Q4_22":
        list_of_name_phone_calls = [call["name"] for call in CALLS][:-1]
        for name in list_of_name_phone_calls:
            if name == "s7d":
                rows = [row for row in data_stat_value["valueRanges"][0]["values"]
                        if row[0] == "step 7" or row[0] == "decision"]
            else:
                rows = [row for row in data_stat_value["valueRanges"][0]["values"] if row[0] == name]
            all_rows = len(rows)
            statuses = len([el[0] for el in rows if len(el) > 7])
            reason_code = len([el[0] for el in rows if len(el) > 8])
            calls_info.append({f"{name}": {"all_rows": all_rows, "statuses": statuses, "reason_code": reason_code}})
    else:
        name = "step 2-3"
        rows = [row for row in data_stat_value["valueRanges"][0]["values"] if row[0] == name]
        all_rows = len(rows)
        statuses = len([el[0] for el in rows if len(el) > 7])
        reason_code = len([el[0] for el in rows if len(el) > 8])
        calls_info.append({f"{name}": {"all_rows": all_rows, "statuses": statuses, "reason_code": reason_code}})

    return {"name": "statuses + reson code", "func": "backed up", "result": "true",
            "info": {"date_for_backup": date_backup_statuses, "detail_info": calls_info}}


def backup_statuses(history_data,
                    history_sheet_id,
                    history_sheet_name,
                    days_before_backup_statuses=DAYS):
    hours_for_backup = (days_before_backup_statuses * 24) + 12
    hours_for_end_row = ((days_before_backup_statuses - 1) * 24) + 12
    date_backup_statuses = (dt.datetime.now() - dt.timedelta(hours=hours_for_backup)).strftime("%Y-%m-%d")
    data_3 = ApiGoogle(history_data, [f"{history_sheet_name}!{date_column}"]).read_data_ranges()
    list_of_dates = []
    for row in data_3["valueRanges"][0]["values"]:
        if not row:
            list_of_dates.append(row)
        else:
            list_of_dates.append(row[0].split(" ")[0])

    start_row = None
    for row in enumerate(list_of_dates):
        if row[1] == date_backup_statuses:
            start_row = row[0] + 1
            break

    end_row = None
    for row in enumerate(list_of_dates):
        if row[1] == (dt.datetime.now() - dt.timedelta(hours=hours_for_end_row)).strftime("%Y-%m-%d"):
            end_row = row[0] + 1
            break

    backup_old_statuses_h_i = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": history_sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": 7,  # столбец Н, по умолчанию 7
                        "endColumnIndex": 9  # столбец I, по умолчанию 9
                    },
                    "destination": {
                        "sheetId": history_sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": 7,  # столбец Н, по умолчанию 7
                        "endColumnIndex": 9  # столбец I, по умолчанию 9
                    },
                    "pasteType": "PASTE_VALUES",  # вставляем как значения, бэкапим статусы через 5 дней
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    }
    ApiGoogle(history_data).copy_paste(backup_old_statuses_h_i)
    return check_backup_statuses(history_data, history_sheet_name, start_row, end_row, date_backup_statuses)


def send_report(list_copying_results, list_add_statuses, start_time, end_time, my_email, password, to_emails):
    message = ""
    message += f"Start time: {start_time}\n\n"
    for result in list_copying_results:
        if result['result'] == "true":
            text = f"The data {result['name'].upper()} was {result['func']} successfully: {result['info']}.\n\n"
        elif result['result'] == "true_2":
            text = f"The data {result['name'].upper()} was NOT {result['func']}: {result['info']}.\n\n"
        else:
            text = f"ERROR: The data {result['name'].upper()} was NOT {result['func']}: {result['info']}.\n\n"
        message += text

    for el in list_add_statuses:
        if all([set(el[0]) == set(el[1]), el[0]["name"] == el[1]["name"],
                el[0]["result"] == el[1]["result"] == "true"]):
            el[0]["info"]["updatedRange"] = ":".join([el[0]["info"]["updatedRange"].split(":")[0],
                                                      el[1]["info"]["updatedRange"].split(":")[1]])
            el[0]["info"]["updatedColumns"] *= 2
            el[0]["info"]["updatedCells"] *= 2

            text_2 = f"Formulas for statuses in {el[0]['name'].upper()} were {el[0]['func']}" \
                     f" successfully: {el[0]['info']}.\n\n"
        else:
            text_2 = f"ERROR: {el[0]}\n\n" \
                     f"ERROR: {el[0]}\n\n"

        message += text_2
    message += f"End time: {end_time}"

    with smtplib.SMTP(host="smtp.gmail.com", port=587) as connection:
        connection.starttls()
        connection.login(user=my_email, password=password)
        connection.sendmail(from_addr=my_email,
                            to_addrs=to_emails,
                            msg=f"Subject:Data update report\n\n{message}")


def main():
    start_time = dt.datetime.utcnow() + dt.timedelta(hours=3)
    list_copying_results = []
    list_add_statuses = []
    for i in range(0, 2):
        try:
            result_backup_st = backup_statuses(
                history_data=HISTORY["history_data"][i],
                history_sheet_id=HISTORY["history_sheet_id"][i],
                history_sheet_name=HISTORY["history_sheet_name"][i])
        except Exception as exc_3:
            result_backup_st = {"name": "statuses + reson code", "func": "backed up", "result": "false", "info": exc_3}
        list_copying_results.append(result_backup_st)

    for call in CALLS:
        try:
            x = 0
            if call["name"] == "abandoned":
                x = 2

            result_copying = copying_main_data(name=call["name"],
                                               call_list=call["call_list"],
                                               history_data=HISTORY["history_data"][x],
                                               history_sheet_id=HISTORY["history_sheet_id"][x],
                                               history_sheet_name=HISTORY["history_sheet_name"][x])
        except Exception as exc_1:
            result_copying = {"name": call["name"], "func": "copied", "result": "false", "info": exc_1}
        list_copying_results.append(result_copying)

        if call["name"] == "step 2-3":
            try:
                result_copying_2 = copying_main_data(name=call["name"],
                                                     call_list=call["call_list"],
                                                     history_data=HISTORY["history_data"][1],
                                                     history_sheet_id=HISTORY["history_sheet_id"][1],
                                                     history_sheet_name=HISTORY["history_sheet_name"][1])
            except Exception as exc_1:
                result_copying_2 = {"name": call["name"], "func": "copied", "result": "false", "info": exc_1}
            list_copying_results.append(result_copying_2)

    for result in list_copying_results:
        if result["result"] == "true" and result["func"] == "copied" and result["name"] != "abandoned":
            try:
                result_add_status = add_statuses_as_formula(result)
            except Exception as exc_2:
                result_add_status = [{"name": result["name"], "func": "added",
                                      "result": "false", "info": exc_2}]
            list_add_statuses.append(result_add_status)
    end_time = dt.datetime.utcnow() + dt.timedelta(hours=3)

    send_report(list_copying_results, list_add_statuses, start_time, end_time, MY_EMAIL, PASSWORD, TO_EMAILS)


if __name__ == "__main__":
    main()