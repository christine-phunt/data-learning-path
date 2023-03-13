import random
from datetime import datetime, timedelta

start_date = datetime(2022, 1, 1)

with open('generated_transactions.csv', 'w') as file:
    file.write('member_id,transaction_type,created_date,price\n')
    for i in range(10000000):
        member_id = random.randint(101, 152)
        transaction_type = random.choice(
            ['Purchase', 'Authorization_only', 'Capture', 'Void', 'Refund', 'Verify'])
        created_date = start_date + timedelta(days=random.randint(0, 364))
        price = round(random.uniform(50, 200), 2)
        file.write(
            f'{member_id},{transaction_type},{created_date.date()},{price}\n')
