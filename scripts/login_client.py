import requests

SERVICE_URL = "http://localhost:8000/login"
AUTH_DATA = {
    "username": "user1",
    "password": "pass1"
}

def login_to_service():
    print(f"Attempting login to: {SERVICE_URL}")

    try:
        response = requests.post(
            SERVICE_URL,
            json=AUTH_DATA
        )

        if response.status_code == 200:
            jwt_token = response.json()
            print("\nSUCCESS: JWT Token received.")
            print(f"Token: {jwt_token}")
            return jwt_token
        
        elif response.status_code == 401:
            print("\nFAILURE: 401 Unauthorized.")
            
        else:
            print(f"\nERROR: HTTP {response.status_code}.")
            
    except requests.exceptions.ConnectionError:
        print("\nCONNECTION ERROR: Ensure the auth service is running at 0.0.0.0:8000.")
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}")

if __name__ == "__main__":
    login_to_service()
