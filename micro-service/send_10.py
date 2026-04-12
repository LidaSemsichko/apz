import requests

URL = "http://localhost:8000/transaction"

for i in range(1, 11):
    payload = {
        "user_id": "user1",
        "amount": 10,
        "transaction_id": f"msg{i}"
    }

    r = requests.post(URL, json=payload)

    print(f"msg{i}: {r.status_code}")
    try:
        print(r.json())
    except Exception:
        print(r.text)