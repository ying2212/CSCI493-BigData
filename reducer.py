import sys
query = sys.argv[1]

# mapping of node ids to names
idToName = {}
with open("nodes.tsv") as f:
    for line in f:
        if line.startswith("id"):
            continue
        parts = line.strip().split('\t')
        node_id, name, node_type = parts[0], parts[1], parts[2]
        idToName[node_id] = name

# Query 1: For each drug, compute the number of genes and the number of diseases associated with the drug
if query == "q1":
    current_drug = None
    gene_count = 0
    disease_count = 0
    drug_data = {}

    # step1: count the number of genes and diseases associated with each drug (CbG, CuG, CdG, CtD, CpD)
    for line in sys.stdin:
        drug, target = line.strip().split('\t')
        if drug != current_drug: # always refresh new drug
            if current_drug:
                drug_data[current_drug] = (gene_count, disease_count)
            current_drug = drug
            gene_count = 0
            disease_count = 0
        if target == "GENE":
            gene_count += 1
        elif target == "DISEASE":
            disease_count += 1
    
    if current_drug:
        drug_data[current_drug] = (gene_count, disease_count)

    # step2: EMIT data 
    print("Query 1: Top 5 Drugs associated to most Gene:")
    print("--------------------------------------------------------")
    print("Drug ID\t\t\t#genes\t\t#Disease")
    print("--------------------------------------------------------")
    
    sorted_drugs = sorted(drug_data.items(), key=lambda x: x[1][0], reverse=True)

    for i in range(min(5, len(sorted_drugs))): 
        drug, (gene, disease) = sorted_drugs[i]
        print(f"{drug} \t\t{gene} \t\t{disease}")



# Query 2: Compute how many diseases are associated with n drugs with top 5 number of diseases
elif query == "q2":
    current_disease = None
    drug_set = set()
    count_diseases = {}

    for line in sys.stdin:
        disease, drug = line.strip().split('\t')
        
        # step1: count the gene of drugs associated with each disease 
        if disease != current_disease:  
            if current_disease is not None: 
                num_drugs = len(drug_set)
                if num_drugs in count_diseases:
                    count_diseases[num_drugs] += 1
                else:
                    count_diseases[num_drugs] = 1
            current_disease = disease
            drug_set = set()

        drug_set.add(drug)

    # step2: add the last disease from the loop 
    if current_disease is not None:
        num_drugs = len(drug_set)
        if num_drugs in count_diseases:
            count_diseases[num_drugs] += 1
        else:
            count_diseases[num_drugs] = 1

     # step3: EMIT data 
    top_counts = sorted(count_diseases.items(), key=lambda x: x[1], reverse=True)
    print("Query 2: Top 5 diseases are associated with with Drugs:")
    print("--------------------------------------------------------")
    for i in range(min(5, len(top_counts))):
        drug_count, disease_total = top_counts[i]
        print(f"{drug_count} drugs -> {disease_total} diseases")



# Query 3: Count number of diseases associated with each gene with top 5 results
elif query == "q3":
    current_drug = None
    gene_count = 0
    gene_counts = {}

    # step1: count the number of genes associated with each drug (CbG, CuG, CdG)
    for line in sys.stdin:
        drug, _ = line.strip().split('\t')
        if drug != current_drug:
            if current_drug:
                gene_counts[current_drug] = gene_count
            current_drug = drug
            gene_count = 0
        gene_count += 1
    #step2: add the last drug from loop
    if current_drug:
        gene_counts[current_drug] = gene_count

    # step3: EMIT data 
    print("Query 3: Top 5 Drugs associated to most Gene:")
    print("--------------------------------------------------------")

    top_genes = sorted(gene_counts.items(), key=lambda x: x[1], reverse=True)

    for i in range(min(5, len(top_genes))):
        gene_id, count = top_genes[i]
        gene_name = idToName.get(gene_id, gene_id)
        print(f"{gene_name} -> {count}")



# cat edges.tsv | python3 mapper.py q1 | sort | python3 reducer.py q1 > Query1.tsv
        
# cat edges.tsv | python3 mapper.py q2 | sort | python3 reducer.py q2 > Query2.tsv
        
# cat edges.tsv | python3 mapper.py q3 | sort | python3 reducer.py q3 > Query3.tsv

