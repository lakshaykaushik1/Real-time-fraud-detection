import pathway as pw

class TransactionSchema(pw.Schema):
    user_id: str
    amount: int
    location: str
    device: str
    timestamp: str

# Read live CSV in streaming mode
transactions = pw.io.csv.read(
    "transactions.csv",
    schema=TransactionSchema,
    mode="streaming"
)

# -------------------------------
# 1 High Amount Risk
# -------------------------------

def high_amount_risk(amount):
    if amount > 30000:
        return 2
    elif amount > 15000:
        return 1
    return 0

# -------------------------------
# 2 Basic Fraud Score
# -------------------------------

scored = transactions.select(
    transactions.user_id,
    transactions.amount,
    transactions.location,
    transactions.device,
    transactions.timestamp,
    amount_risk=pw.apply(high_amount_risk, transactions.amount)
)

# -------------------------------
# 3 Rolling Average Per User
# -------------------------------

avg_per_user = transactions.groupby(
    transactions.user_id
).reduce(
    transactions.user_id,
    avg_amount=pw.reducers.avg(transactions.amount)
)

# Join back with transactions
joined = scored.join(
    avg_per_user,
    scored.user_id == avg_per_user.user_id
).select(
    scored.user_id,
    scored.amount,
    scored.location,
    scored.device,
    scored.timestamp,
    scored.amount_risk,
    avg_per_user.avg_amount
)

# -------------------------------
# 4 Deviation Risk
# -------------------------------

def deviation_risk(amount, avg_amount):
    if avg_amount is None:
        return 0
    if amount > avg_amount * 2:
        return 2
    return 0

final = joined.select(
    joined.user_id,
    joined.amount,
    joined.location,
    joined.device,
    joined.timestamp,
    joined.amount_risk,
    joined.avg_amount,
    deviation_risk=pw.apply(deviation_risk, joined.amount, joined.avg_amount),
)

# -------------------------------
# 5 Total Fraud Score
# -------------------------------

result = final.select(
    final.user_id,
    final.amount,
    final.location,
    final.device,
    final.timestamp,
    fraud_score=final.amount_risk + final.deviation_risk
)

# Output live
pw.io.stdout.write(result)

pw.run()