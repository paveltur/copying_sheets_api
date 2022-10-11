from authentication import ApiGoogle
import datetime as dt
import smtplib
import json

with open("creds_sheets.json", "r") as f:
    creds_sheets = json.load(fp=f)

phone_calls = creds_sheets["phone_calls"]

HISTORY = creds_sheets["HISTORY"]

column_a = "A2:A"
column_cd = "C2:D"
column_m = "M2:M"
columns_for_date = ["Q4_22!G2:G"]
range_for_copy = "A2:U"
MY_EMAIL = creds_sheets["mail_creds"]["MY_EMAIL"]
PASSWORD = creds_sheets["mail_creds"]["PASSWORD"]
TO_EMAILS = creds_sheets["mail_creds"]["TO_EMAILS"]


def copying_main_data(name,
                      call_list,
                      range_cd=column_cd,
                      range_a=column_a,
                      range_copy=range_for_copy,
                      history_data=HISTORY["history_data"],
                      history_sheet_id=HISTORY["history_sheet_id"],
                      history_range_read=HISTORY["history_range_read"]):

    num_rows = len(ApiGoogle(call_list, f"{name}!{range_cd}").read_data_ranges()["valueRanges"][0]["values"])
    if name == "pdl_old" or name == "il":
        ApiGoogle(call_list, f"{name}!{range_a}").update_data({"values": [[f'{name}']]*num_rows})

    data_1 = ApiGoogle(history_data, history_range_read).read_data_ranges()
    data_2 = ApiGoogle(call_list, [f"{name}!{range_copy}{num_rows+1}"]).read_data_ranges()

    try:
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
        for row in data_2["valueRanges"][0]["values"]:
            try:
                if row[0] == "step 2-3" and row[12]:
                    list_of_rows.append(row)
            except IndexError:
                continue
        try:
            last_hist_value = [el for el in list_for_last_hist_values if el[0] == f"{name}"][-1]
        except IndexError:
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
        last_row_history_list = len(data_1['valueRanges'][0]['values']) + 2
    except KeyError:
        last_row_history_list = 2

    range_for_writing = f"Q4_22!A{last_row_history_list}:U"

    if not range_body["values"]:
        return {"name": name, "func": "copied", "result": "true_2", "info": "no data to copy"}
    else:
        ApiGoogle(history_data).add_1_row_end(history_sheet_id)
        add_new_data = ApiGoogle(history_data, range_for_writing).update_data(range_body)
        return {"name": name, "func": "copied", "result": "true", "info": add_new_data}


def add_statuses_as_formula(data: dict, history_data=HISTORY["history_data"]):
    nums_cells = []
    list_cells = data["info"]["updatedRange"].split("!")[1].split(":")
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

    info_for_writing = [{"range_for_writing": f"Q4_22!H{nums_cells[0]}:H{nums_cells[1]}",
                         "body": {"values": formulas_h_column}},
                        {"range_for_writing": f"Q4_22!I{nums_cells[0]}:I{nums_cells[1]}",
                         "body": {"values": formulas_i_column}}]
    result = []
    for column in info_for_writing:
        statuses_results = ApiGoogle(history_data, column["range_for_writing"]).update_data(column["body"])
        result.append({"name": data["name"], "func": "added",
                       "result": "true", "info": statuses_results})
    return result


def backup_statuses(history_data=HISTORY["history_data"],
                    history_sheet_id=HISTORY["history_sheet_id"],
                    days_before_backup_statuses=5,
                    start_row=None,
                    end_row=None):

    hours_for_backup = (days_before_backup_statuses * 24) + 12
    hours_for_end_row = ((days_before_backup_statuses - 1) * 24) + 12
    date_backup_statuses = (dt.datetime.now() - dt.timedelta(hours=hours_for_backup)).strftime("%Y-%m-%d")
    data_3 = ApiGoogle(history_data, columns_for_date).read_data_ranges()
    list_of_dates = []
    for row in data_3["valueRanges"][0]["values"]:
        if not row:
            list_of_dates.append(row)
        else:
            list_of_dates.append(row[0].split(" ")[0])

    for row in enumerate(list_of_dates):
        if row[1] == date_backup_statuses:
            start_row = row[0] + 1
            break

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

    list_of_name_phone_calls = [call["name"] for call in phone_calls]
    calls_info = []
    data_stat_value = ApiGoogle(history_data, [f"Q4_22!A{start_row+1}:I{end_row}"]).read_data_ranges()
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

    return {"name": "statuses + reson code", "func": "backed up", "result": "true",
            "info": {"date_for_backup": date_backup_statuses, "from_cell": f"H{start_row+1}", "to_cell": f"I{end_row}",
                     "detail_info": calls_info}}


def send_report(copying_results, list_add_statuses, start_time, end_time, my_email, password, to_emails):
    message = ""
    message += f"Start time: {start_time}\n\n"
    for result in copying_results:
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
    start_time = dt.datetime.now()
    copying_results = []
    list_add_statuses = []
    for key in phone_calls:
        try:
            result_copying = copying_main_data(key["name"], key["call_list"])
        except Exception as exc_1:
            result_copying = {"name": key["name"], "func": "copied", "result": "false", "info": exc_1}
        copying_results.append(result_copying)

        if result_copying["result"] == "true":
            try:
                result_add_status = add_statuses_as_formula(result_copying)
            except Exception as exc_2:
                result_add_status = [{"name": result_copying["name"], "func": "added",
                                      "result": "false", "info": exc_2}]
            list_add_statuses.append(result_add_status)
    try:
        result_backup_st = backup_statuses()
    except Exception as exc_3:
        result_backup_st = {"name": "statuses + reson code", "func": "backed up", "result": "false", "info": exc_3}
    copying_results.append(result_backup_st)
    end_time = dt.datetime.now()

    send_report(copying_results, list_add_statuses, start_time, end_time, MY_EMAIL, PASSWORD, TO_EMAILS)


if __name__ == "__main__":
    main()