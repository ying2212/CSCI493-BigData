import sys


# Query 1:
# For each drug, compute the number of genes and number of diseases associated with the drug.
# output the top 5 drugs by number of genes in descending order.

# Query 2:
# Compute the number of diseases associated with 1, 2, 3, ..., n drugs. 
# Output results with the top 5 number of diseases in a descending order.

# Query 3:
# Get the name of drugs with the top 5 number of genes associated with them.


query = sys.argv[1]  

for line in sys.stdin:
    if line.startswith("source"):
        continue
    parts = line.strip().split('\t') 
    if len(parts) != 3:
        continue
    source, metaedge, target = parts
    
    if query == "q1":
        if metaedge in {"CbG", "CdG", "CuG"}: 
            print(f"{source}\tGENE")  # Emit (source, GENE)
        elif metaedge in {"CtD", "CpD"}: 
            print(f"{source}\tDISEASE")  # Emit (source, DISEASE)
    
    elif query == "q2":
        if metaedge in {"CtD", "CpD"}: 
            print(f"{target}\t{source}")  # Emit (target disease, source drug)
    
    elif query == "q3":
        if metaedge in {"CbG", "CuG", "CdG"}:
            print(f"{source}\tGENE")  # Emit (source, GENE)

