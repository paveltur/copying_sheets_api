from authentication import ApiGoogle
import datetime as dt
import smtplib
import os

PDL_NEW_HISTORY = os.environ.get("PDL_NEW_HISTORY")  # Исторические данные pdl_new
PDL_NEW_HISTORY_SHEET_ID = os.environ.get("PDL_NEW_HISTORY_SHEET_ID")  # Q3_22
PDL_NEW_CALL_LIST_S7D = os.environ.get("PDL_NEW_CALL_LIST_S7D")  # Обзвон отвалившихся S7D
PDL_NEW_CALL_LIST_S23 = os.environ.get("PDL_NEW_CALL_LIST_S23") # Обзвон отвалившихся S23
PDL_NEW_RANGES_FOR_HISTORY = ["Q3_22!A1:C", "Q3_22!G1:G"]  # поиск последнего значения исторические данные pdl_new
PDL_NEW_RANGE_FOR_CALL_LIST = ["Лист1!A2:T"]  # для захвата результатов обзвона

PDL_OLD_HISTORY = os.environ.get("PDL_OLD_HISTORY")  # Исторические данные pdl_old
PDL_OLD_HISTORY_SHEET_ID = os.environ.get("PDL_OLD_HISTORY_SHEET_ID")  # Q3_22
PDL_OLD_CALL_LIST = os.environ.get("PDL_OLD_CALL_LIST")  # Обзвон отвалившихся
PDL_OLD_RANGE_FOR_HISTORY = ["Q3_22!C1:C"]  # для поиска последнего значения исторические данные pdl_old
PDL_OLD_RANGE_FOR_CALL_LIST = ["PDL c 01.08!A2:N"]  # для выгрузки результатов обзвона

IL_HISTORY = os.environ.get("IL_HISTORY")
IL_HISTORY_SHEET_ID = os.environ.get("IL_HISTORY_SHEET_ID")  # Q3_22
IL_CALL_LIST = os.environ.get("IL_CALL_LIST")
IL_RANGE_FOR_HISTORY = ["Q3_22!D1:D"]  # для поиска последнего значения исторические данные IL
IL_RANGE_FOR_CALL_LIST = ["IL since 01.08.2022!A2:O",  # для выгрузки результатов обзвона, первый в списке
                          "IL since 01.08.2022!D2:D",
                          "IL since 01.08.2022!I2:I"]

PRE_APP_HISTORY = os.environ.get("PRE_APP_HISTORY")
PRE_APP_HISTORY_SHEET_ID = os.environ.get("PRE_APP_HISTORY_SHEET_ID")  # Q3_22
PRE_APP_CALL_LIST = os.environ.get("PRE_APP_CALL_LIST")
PRE_APP_RANGE_FOR_HISTORY = ["Q3_22!A1:A"]  # для поиска последнего значения исторические данные предодобренные
PRE_APP_RANGE_FOR_CALL_LIST = ["Sheet1!A2:P",  # для выгрузки результатов обзвона, первый в списке
                               "Sheet1!A2:A",
                               "Sheet1!H2:H"]

MY_EMAIL = os.environ.get("MY_EMAIL")
PASSWORD = os.environ.get("PASSWORD")
TO_EMAILS = [MY_EMAIL, os.environ.get("pavel"), os.environ.get("nataliya"), os.environ.get("diana")]  #


def pdl_new_update(history, history_sheet_id, ranges_for_history, call_list_s7d, call_list_s23, ranges_for_call_list,
                   copy_paste_start_column_index=7, copy_paste_end_column_index=8, days_before_backup_statuses=5):
    """
    запрос в исторические данные для получения последнего скопированного значения, формирования диапазона для записи,
    а также диапазона строк для копирования статусов в столбце H из формулы в значение
    """
    data_1 = ApiGoogle(history, ranges_for_history).read_data_ranges()
    try:
        list_for_last_values = data_1["valueRanges"][0]["values"]
    except KeyError:
        list_for_last_values = []

    try:
        last_value_s7d = [row[2] for row in list_for_last_values if row[0] == "step 7" or row[0] == "decision"][-1]
    except IndexError:
        last_value_s7d = None
    try:
        last_value_s23 = [row[2] for row in list_for_last_values if row[0] == "step 2-3"][-1]
    except IndexError:
        last_value_s23 = None

    data_2 = ApiGoogle(call_list_s7d, ranges_for_call_list).read_data_ranges()
    list_of_rows_s7d = [row for row in data_2["valueRanges"][0]["values"] if row[0] == "step 7" or row[0] == "decision"]
    if last_value_s7d != None:
        index_cut_list_s7d = int([list_of_rows_s7d.index(row) for row in list_of_rows_s7d
                                  if row[2] == last_value_s7d][0]) + 1
    else:
        index_cut_list_s7d = None

    data_3 = ApiGoogle(call_list_s23, ranges_for_call_list).read_data_ranges()
    list_of_rows_s23 = []
    for row in data_3["valueRanges"][0]["values"]:
        try:
            if row[0] == "step 2-3" and row[12]:
                list_of_rows_s23.append(row)
        except IndexError:
            continue
    if last_value_s23 != None:
        index_cut_list_s23 = int([list_of_rows_s23.index(row) for row in list_of_rows_s23
                              if row[2] == last_value_s23][0]) + 1
    else:
        index_cut_list_s23 = None

    range_body = {"values": (list_of_rows_s7d[index_cut_list_s7d:]+list_of_rows_s23[index_cut_list_s23:])}

    list_name = ranges_for_history[0].split('!')[0]  # получаем имя листа с истор. (для записи в дальнейшем)
    try:
        last_row_history_list = len(data_1['valueRanges'][0]['values'])  # получаем номер последней строки истор.
    except KeyError:
        last_row_history_list = 0
    part_of_range_wr = []
    for char in ranges_for_call_list[0].split("!")[1]:
        if char.isalpha():
            part_of_range_wr.append(char)
        elif char.isdigit():
            if str(last_row_history_list+1) not in part_of_range_wr:
                part_of_range_wr.append(str(last_row_history_list+1))
        else:
            part_of_range_wr.append(char)
    range_for_writing = f"{list_name}!{''.join(part_of_range_wr)}"  # получаем итоговый диапазон для записи данных
    if last_row_history_list !=0:
        copy_start_row_index = int(len(data_1["valueRanges"][0]["values"])) - 1
        copy_end_row_index = int(len(data_1["valueRanges"][0]["values"]))
        update_statuses_as_formula_h = {
            "requests": [
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": history_sheet_id,
                            "startRowIndex": copy_start_row_index,
                            "endRowIndex": copy_end_row_index,
                            "startColumnIndex": copy_paste_start_column_index,  # столбец Н, по умолчанию 7
                            "endColumnIndex": copy_paste_end_column_index  # столбец Н, по умолчанию 8
                        },
                        "destination": {
                            "sheetId": history_sheet_id,
                            "startRowIndex": copy_end_row_index,  # тянем формулу статусов с последней строки
                            "endRowIndex": (copy_end_row_index + (int(len(range_body["values"])))),  # на весь массив
                            "startColumnIndex": copy_paste_start_column_index,  # столбец Н, по умолчанию 7
                            "endColumnIndex": copy_paste_end_column_index  # столбец Н, по умолчанию 8
                        },
                        "pasteType": "PASTE_FORMULA",  # формулу для статусов вставляем как формулу
                        "pasteOrientation": "NORMAL"
                    }
                }
            ]
        }
    try:
        only_date_list = [row[0].split(" ")[0] for row in data_1["valueRanges"][1]["values"]]  # формируем лист с датами
    except KeyError:
        only_date_list = []
    finally:
        hours = ((days_before_backup_statuses * 24) + 12)
        date_backup_statuses = (dt.datetime.now()-dt.timedelta(hours=hours)).strftime("%Y-%m-%d")
        # формируем список с номерами строк для копирования статуса из формулы в значение
        list_for_day = [(date[0]) for date in enumerate(only_date_list) if date[1] == date_backup_statuses]

    if int(len(list_for_day)) > 0:
        start_row_for_backup_old_statuses = list_for_day[0]  # получаем первую строку диапазона для статусов
        end_row_for_backup_old_statuses = list_for_day[-1] + 1  # получаем последнюю строку диапазона для статусов
        backup_old_statuses_h = {
            "requests": [
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": history_sheet_id,
                            "startRowIndex": start_row_for_backup_old_statuses,
                            "endRowIndex": end_row_for_backup_old_statuses,
                            "startColumnIndex": copy_paste_start_column_index,  # столбец Н, по умолчанию 7
                            "endColumnIndex": copy_paste_end_column_index  # столбец Н, по умолчанию 8
                        },
                        "destination": {
                            "sheetId": history_sheet_id,
                            "startRowIndex": start_row_for_backup_old_statuses,
                            "endRowIndex": end_row_for_backup_old_statuses,
                            "startColumnIndex": copy_paste_start_column_index,  # столбец Н, по умолчанию 7
                            "endColumnIndex": copy_paste_end_column_index  # столбец Н, по умолчанию 8
                        },
                        "pasteType": "PASTE_VALUES",  # вставляем как значения, бэкапим статусы через 5 дней
                        "pasteOrientation": "NORMAL"
                    }
                }
            ]
        }
        ApiGoogle(history).copy_paste(backup_old_statuses_h)
    if not range_body["values"]:
        return ["PDL_NEW", "true_1", "no data to copy."]
    else:
        ApiGoogle(history).add_1_row_end(history_sheet_id)
        add_new_data = ApiGoogle(history, range_for_writing).update_data(range_body)
        if int(len(list_for_day)) > 0:
            ApiGoogle(history).copy_paste(update_statuses_as_formula_h)
        return ["PDL_NEW", "true_2", add_new_data, len(list_of_rows_s7d[index_cut_list_s7d:]),
                len(list_of_rows_s23[index_cut_list_s23:])]


def pdl_old_update(history, history_sheet_id, ranges_for_history, call_list, ranges_for_call_list):
    data_1 = ApiGoogle(history, ranges_for_history).read_data_ranges()  # запрос в исторические данные
    last_value = data_1["valueRanges"][0]["values"][-1][0]  # получаем последнее значение исторических данных
    list_name = ranges_for_history[0].split('!')[0]  # получаем имя листа с истор. (для записи в дальнейшем)
    last_row_history_list = len(data_1['valueRanges'][0]['values'])  # получаем номер последней строки истор.
    part_of_range_wr = []
    for char in ranges_for_call_list[0].split("!")[1]:
        if char.isalpha():
            part_of_range_wr.append(char)
        elif char.isdigit():
            if str(last_row_history_list + 1) not in part_of_range_wr:
                part_of_range_wr.append(str(last_row_history_list + 1))
        else:
            part_of_range_wr.append(char)
    range_for_writing = f"{list_name}!{''.join(part_of_range_wr)}"  # получаем итоговый диапазон для записи данных

    # запрос в список обзвона для формирования массива данных для копирования
    data_2 = ApiGoogle(call_list, ranges_for_call_list).read_data_ranges()
    list_of_row = [row for row in data_2["valueRanges"][0]["values"] if row[0] != ""]
    # поиск в массиве индекса последнего значения из исторических данных
    index_cut_list = [list_of_row.index(row) for row in list_of_row if row[2] == last_value][0] + 1

    range_body = {"values": list_of_row[index_cut_list:]}
    if not range_body["values"]:
        return ["PDL_OLD", "true_1", "no data to copy."]
    else:
        ApiGoogle(history).add_1_row_end(history_sheet_id)
        add_new_data = ApiGoogle(history, range_for_writing).update_data(range_body)
        return ["PDL_OLD", "true_2", add_new_data]


def il_plus_pre_approved_update(history, ranges_for_history, history_sheet_id, call_list, ranges_for_call_list, name):
    data_1 = ApiGoogle(history, ranges_for_history).read_data_ranges()  # запрос в исторические данные
    last_value = data_1["valueRanges"][0]["values"][-1][0]  # получаем последнее значение исторических данных
    list_name = ranges_for_history[0].split('!')[0]  # получаем имя листа с истор. (для записи в дальнейшем)
    last_row_history_list = len(data_1['valueRanges'][0]['values'])  # получаем номер последней строки истор.
    part_of_range_wr = []
    for char in ranges_for_call_list[0].split("!")[1]:
        if char.isalpha():
            part_of_range_wr.append(char)
        elif char.isdigit():
            if str(last_row_history_list + 1) not in part_of_range_wr:
                part_of_range_wr.append(str(last_row_history_list + 1))
        else:
            part_of_range_wr.append(char)
    range_for_writing = f"{list_name}!{''.join(part_of_range_wr)}"  # получаем итоговый диапазон для записи данных

    data_2 = ApiGoogle(call_list, ranges_for_call_list).read_data_ranges()
    index_cut_list = data_2["valueRanges"][1]["values"].index([last_value]) + 1
    last_called_index = int(len(data_2["valueRanges"][2]["values"]))
    range_body = {"values": data_2["valueRanges"][0]["values"][index_cut_list:last_called_index]}

    if not range_body["values"]:
        return [name, "true_1", "no data to copy."]
    else:
        ApiGoogle(history).add_1_row_end(history_sheet_id)
        add_new_data = ApiGoogle(history, range_for_writing).update_data(range_body)
        return [name, "true_2", add_new_data]


def send_mail(time_start, time_end, my_email, password, to_emails, results):
    final_text = []
    for item in results:
        if item[1] == "true_1":
            text = f"\n{item[0]} - {item[2]}\n"
        elif item[1] == "true_2":
            text = f"\n{item[0]} was copied successfully.\n" \
                   f"Updated range: {item[2]['updatedRange']}, updated rows: {item[2]['updatedRows']}.\n"
            if item[0] == "PDL_NEW":
                text += f"Statuses were backed up for " \
                        f"{(dt.datetime.now()-dt.timedelta(hours=((5*24)+12))).strftime('%Y-%m-%d')}\n" \
                        f"Step 7 + Decision: {item[3]} rows, Step 2-3: {item[4]} rows.\n"
        else:
            text = f"\n{item[0]} - copy error.\n"
        final_text.append(text)
    message = f"Start of copying: {time_start}\nEnd of copying: {time_end}\n{''.join(final_text)}"

    with smtplib.SMTP(host="smtp.gmail.com", port=587) as connection:
        connection.starttls()
        connection.login(user=my_email, password=password)
        connection.sendmail(from_addr=my_email,
                            to_addrs=to_emails,
                            msg=f"Subject:Data update report\n\n{message}")


def main():
    start_time = dt.datetime.strftime(dt.datetime.now(), "%d-%m-%Y %H:%M:%S")
    try:
        pdl_new = pdl_new_update(history=PDL_NEW_HISTORY,
                                 ranges_for_history=PDL_NEW_RANGES_FOR_HISTORY,
                                 history_sheet_id=PDL_NEW_HISTORY_SHEET_ID,
                                 call_list_s7d=PDL_NEW_CALL_LIST_S7D,
                                 call_list_s23=PDL_NEW_CALL_LIST_S23,
                                 ranges_for_call_list=PDL_NEW_RANGE_FOR_CALL_LIST)
    except:
        pdl_new = ["PDL_NEW", "false"]

    try:
        pdl_old = pdl_old_update(history=PDL_OLD_HISTORY,
                                 ranges_for_history=PDL_OLD_RANGE_FOR_HISTORY,
                                 history_sheet_id=PDL_OLD_HISTORY_SHEET_ID,
                                 call_list=PDL_OLD_CALL_LIST,
                                 ranges_for_call_list=PDL_OLD_RANGE_FOR_CALL_LIST)
    except:
        pdl_old = ["PDL_OLD", "false"]

    try:
        il = il_plus_pre_approved_update(history=IL_HISTORY,
                                         ranges_for_history=IL_RANGE_FOR_HISTORY,
                                         history_sheet_id=IL_HISTORY_SHEET_ID,
                                         call_list=IL_CALL_LIST,
                                         ranges_for_call_list=IL_RANGE_FOR_CALL_LIST,
                                         name="IL")
    except:
        il = ["IL", "false"]

    try:
        pre_approved = il_plus_pre_approved_update(history=PRE_APP_HISTORY,
                                                   ranges_for_history=PRE_APP_RANGE_FOR_HISTORY,
                                                   history_sheet_id=PRE_APP_HISTORY_SHEET_ID,
                                                   call_list=PRE_APP_CALL_LIST,
                                                   ranges_for_call_list=PRE_APP_RANGE_FOR_CALL_LIST,
                                                   name="PRE-APPROVED")
    except:
        pre_approved = ["PRE-APPROVED", "false"]

    end_time = dt.datetime.strftime(dt.datetime.now(), "%d-%m-%Y %H:%M:%S")
    results = [pdl_new, pdl_old, il, pre_approved]
    send_mail(time_start=start_time, time_end=end_time, my_email=MY_EMAIL, password=PASSWORD, to_emails=TO_EMAILS,
              results=results)


if __name__ == "__main__":
    main()
