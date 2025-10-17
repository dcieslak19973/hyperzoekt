def resource_user():
    try:
        x = 1 / 0
    finally:
        print("cleanup")
