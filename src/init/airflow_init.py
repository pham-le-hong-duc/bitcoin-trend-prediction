import subprocess
import sys
import time

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}", file=sys.stderr)
        print(f"Output: {e.stdout}", file=sys.stderr)
        return False

def main():
    print("AIRFLOW INITIALIZATION")
    
    # Run db init to create database schema
    print("Initializing database...")
    if not run_command("airflow db init"):
        sys.exit(1)
    
    # Create admin user
    print("Creating admin user...")
    create_user_cmd = (
        "airflow users create "
        "--username admin "
        "--firstname Admin "
        "--lastname User "
        "--role Admin "
        "--email admin@example.com "
        "--password password"
    )
    
    result = subprocess.run(create_user_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 and "already exists" not in result.stdout and "already exists" not in result.stderr:
        print(f"Warning: {result.stderr}")
    
    print("AIRFLOW INITIALIZATION COMPLETED!")

if __name__ == "__main__":
    main()
