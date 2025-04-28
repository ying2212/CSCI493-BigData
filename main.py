import subprocess

def run_query(query):
    if query not in {"q1", "q2", "q3"}:
        print("Invalid query. Please enter q1, q2, or q3.")
        return

    try:
        cmd = f"cat edges.tsv | python3 mapper.py {query} | sort | python3 reducer.py {query}"
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running query: {e}")

if __name__ == "__main__":
    query = input("Which query do you want to run? (q1/q2/q3): ").strip().lower()
    run_query(query)
