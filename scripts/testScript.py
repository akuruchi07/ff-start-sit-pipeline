from lambda_function import lambda_handler

event = {
    "season": 2024,
    "week": 3
}

lambda_handler(event, None)