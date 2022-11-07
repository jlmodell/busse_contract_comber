import smtplib
import pandas as pd
import os
from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
mongodb_uri = (
    "mongodb+srv://jmodell:5T1aBQa7@busseforce0.9fmow.mongodb.net/bussepricing"
)
client = MongoClient(mongodb_uri)

# Get the database
db = client.get_database("bussepricing")

# Get the collection
contracts = db.get_collection("contract_prices")
costs = db.get_collection("costs")
customers = db.get_collection("customers")


def main():
    # Get user input
    item = input("Enter item: ")
    contractend = input("Enter contract end date YYYY-MM-DD: ")

    try:
        contractend = datetime.strptime(contractend, "%Y-%m-%d")
    except ValueError:
        print("Incorrect data format, should be YYYY-MM-DD")
        exit()

    # Get the data
    contracts_df = pd.DataFrame(
        list(
            contracts.find(
                {
                    "pricingagreements.item": item,
                    "contractend": {
                        "$gte": contractend,
                    },
                },
                {
                    "pricingagreements": 1,
                    "contractend": 1,
                    "contractname": 1,
                    "contractnumber": 1,
                    "_id": 0,
                    "contractstart": 1,
                },
            )
        )
    )

    # Filter the data
    contracts_df = filter_pricingagreements(contracts_df, item)

    # Get the cost
    contracts_df["item"] = item
    contracts_df["cost"] = round(contracts_df["item"].apply(get_cost), 2)
    contracts_df["customer_fee%"] = round(
        contracts_df["contractname"].apply(get_customer), 2
    )
    contracts_df["safety"] = round(contracts_df["cost"] * 0.05, 2)
    contracts_df["customer_fee"] = round(
        (contracts_df["customer_fee%"] * contracts_df["pricingagreements"]), 2
    )
    contracts_df["total_cost"] = round(
        (contracts_df["cost"] + contracts_df["customer_fee"] + contracts_df["safety"]),
        2,
    )

    # Calculate the GP
    contracts_df["gp"] = round(
        contracts_df["pricingagreements"] - contracts_df["total_cost"], 2
    )

    # Calculate the GP %
    contracts_df["gp%"] = round(
        contracts_df["gp"] / contracts_df["pricingagreements"] * 100, 2
    )

    # Flag for review
    contracts_df["review"] = contracts_df["gp%"] < 26.9999

    # Sort the data
    contracts_df = contracts_df.sort_values(by=["contractend", "review"])

    # Save the data
    save_path = os.path.join(
        "C:\\", "temp", f"{item} contracts expiring after {contractend:%Y-%m-%d}.xlsx"
    )
    contracts_df.to_excel(save_path, index=False)

    print("Done")
    print("Saved to", save_path)

    return contracts_df


def filter_pricingagreements(df, item):
    df["pricingagreements"] = df["pricingagreements"].apply(
        lambda x: list(filter(lambda y: y["item"] == item, x))[0].get("price")
    )
    return df


def get_cost(item):
    cost = costs.find_one({"item": item})
    if not cost:
        raise ValueError("Item not found")
    return cost["cost"]


def get_customer(customerName):
    customer = customers.find_one({"contract_name": customerName})
    if not customer:
        customer = {}

    return (
        customer.get("distributor_fee", 0.05)
        + customer.get("cash_discount_fee", 0.00)
        + customer.get("gpo_fee", 0.00)
    )


if __name__ == "__main__":
    df = main()
