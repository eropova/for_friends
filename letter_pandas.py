import pandas as pd
import datetime as dt

letter_file = "D:/doggy/letters_veeam_2.html"

letter_read = pd.read_html(letter_file)

letter_data = letter_read[2]

month = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}

values = {'MB': 1024, 'GB': 1048576, }

day = letter_read[1].iloc[1, 0].split('.')[0].replace(' г', '').split(' ')
day = (int(day[2]), month.get(day[1][:3]), int(day[0]))

for _ in range(6, letter_data.shape[1]):
    statuses = {'Success': 0, 'Warning': 0, 'Error': 0}
    name = letter_data.iloc[_, 0]
    status = letter_data.iloc[_, 1]
    statuses[status] += 1
    st_time = [int(x) for x in letter_data.iloc[_, 2].split(':')]
    st_time_epoch = dt.datetime(*day, *st_time).timestamp()
    end_time = [int(x) for x in letter_data.iloc[_, 3].split(':')]
    end_time_epoch = dt.datetime(*day, *end_time).timestamp()
    transferred = letter_data.iloc[_, 6].split(' ')
    transferred_kb = float(transferred[0].replace(',', '.')) * values.get(transferred[1])
    with open('D:/piggy/learn_python/venv/mt/bc_dataprotector_veeam.prom', 'a') as file:
        file.write(
            f'bc_dpbackup_start{{source="{name}", system="veeam", period="daily"}} {int(st_time_epoch)} \n'
            f'bc_dpbackup_end{{source="{name}", system="veeam", period="daily"}} {int(end_time_epoch)} \n'
            f'bc_dpbackup_complieted{{source="{name}", system="veeam", period="daily"}} {statuses["Success"]} \n'
            f'bc_dpbackup_size{{source="{name}", system="veeam", period="daily"}} {int(transferred_kb)} \n'
            f'bc_dpbackup_warnings{{source="{name}", system="veeam", period="daily"}} {statuses["Warning"]} \n'
            f'bc_dpbackup_errors{{source="{name}", system="veeam", period="daily"}} {statuses["Error"]} \n'
            '\n'

        )
    print(day, name, status, int(st_time_epoch), int(end_time_epoch), int(transferred_kb))
