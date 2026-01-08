from datetime import datetime

date_format = '%d-%m-%Y'
CATEGORIES = {"I": "Income", "E":"Expense"}

def get_date(prompt, allow_default=False):
    date_str = input(prompt)
    if allow_default and not date_str:
        return datetime.today().strftime(date_format)
    
    try:

        valid_date = datetime.strptime(date_str, date_format)
        return valid_date.strftime(date_format)
    except ValueError:
        print("Invalid date format. Please enter the date in format dd-mm-yyyy format")
        return get_date(prompt, allow_default)

def get_amount():
    amount_str = input("Enter the amount: ").strip()

    if not amount_str:  # handle empty input
        print("Amount cannot be empty.")
        return get_amount()
    try:
        amount = float(amount_str)
        if amount <= 0:
            raise ValueError("Amount must be non-negative,non-zero value")
        return amount
    except ValueError as e:
        print(e)
        return get_amount()

def get_category():
    category = input("Enter the category ('I' for income and 'E' for expenses): ").upper()
    if category in CATEGORIES:
        return CATEGORIES[category] 
    
    print("Invalid categories. Please enter 'I' for income and 'E' for expense")
    return get_category()


def get_description():
    return input("Enter a description (optional): ")
