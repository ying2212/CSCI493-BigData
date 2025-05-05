from pyspark.sql import SparkSession

def query1(edges_rdd):
    # filter for needed metaedges
    needed_rdd = edges_rdd.filter(lambda row: row[1] in ("CbG", "CuG", "CdG", "CtD", "CpD"))

    # count genes per drug (CbG, CuG, CdG)
    gene_rdd = needed_rdd.filter(lambda row: row[1] in ("CbG", "CuG", "CdG")) \
                         .map(lambda row: (row[0], 1)) \
                         .reduceByKey(lambda a, b: a + b)  # (drug, gene_count)

    #count diseases per drug (CtD, CpD)
    disease_rdd = needed_rdd.filter(lambda row: row[1] in ("CtD", "CpD")) \
                            .map(lambda row: (row[0], 1)) \
                            .reduceByKey(lambda a, b: a + b)  # (drug, disease_count)

    #join gene and disease counts
    combined_rdd = gene_rdd.fullOuterJoin(disease_rdd) \
                           .mapValues(lambda x: (x[0] if x[0] else 0, x[1] if x[1] else 0))  # Fill missing with 0

    # Sort and take top 5 drugs by gene count
    top5 = combined_rdd.takeOrdered(5, key=lambda x: -x[1][0])

    #emit
    print("Query 1: Top 5 Drugs associated to most Gene:")
    print("--------------------------------------------------------")
    print("Drug ID\t\t#genes\t\t#diseases")
    print("--------------------------------------------------------")
    for drug_id, (gene_count, disease_count) in top5:
        print(f"{drug_id}\t{gene_count}\t\t{disease_count}")


def query2(edges_rdd):
    # Filter
    needed_rdd = edges_rdd.filter(lambda row: row[1] in ("CtD", "CpD"))
    
    # Map to (disease_id, drug_id)
    disease_drugs = needed_rdd.map(lambda row: (row[2], row[0])).distinct()

    # Count drugs per disease
    disease_counts = disease_drugs.map(lambda row: (row[0], 1)) \
                                  .reduceByKey(lambda a, b: a + b)  # (disease_id, num_drugs)

    # count distinct diseases per drug
    drug_count_groups = disease_counts.map(lambda row: (row[1], 1)) \
                                      .reduceByKey(lambda a, b: a + b)  # (num_drugs, num_diseases)

    # sort and take top 5
    top5 = drug_count_groups.takeOrdered(5, key=lambda x: -x[1])

    # Emit
    print("Query 2: Top 5 Disease Counts with Drugs:")
    print("--------------------------------------------------------")
    for num_drugs, num_diseases in top5:
        print(f"{num_drugs} drugs -> {num_diseases} diseases")


def query3(edges_rdd, nodes_rdd):
    # filter for needed metaedges
    gene_edges = edges_rdd.filter(lambda row: row[1] in ("CbG", "CuG", "CdG"))

    #count genes per drug by mapping to (drug_id, 1)
    gene_counts = gene_edges.map(lambda row: (row[0], 1)) \
                            .reduceByKey(lambda a, b: a + b)  # (drug_id, gene_count)

    #map nodes to (id, name)
    drug_names = nodes_rdd.map(lambda row: (row[0], row[1]))  # (id, name)

    # Join drug names with gene counts
    joined = gene_counts.join(drug_names)

    # sort by gene count and take top 5 result
    top5 = joined.takeOrdered(5, key=lambda x: -x[1][0])

    # emit
    print("Query 3: Top 5 Drugs associated to most Gene:")
    print("--------------------------------------------------------")
    for _, (gene_count, name) in top5:
        print(f"{name} -> {gene_count}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("BigDataProject2").getOrCreate()
    sc = spark.sparkContext

    edges_rdd = sc.textFile("edges.tsv").map(lambda line: line.split("\t")).filter(lambda row: row[0] != "source")
    nodes_rdd = sc.textFile("nodes.tsv").map(lambda line: line.split("\t")).filter(lambda row: row[0] != "id")

    query = input("Which query do you want to run? (q1/q2/q3): ").strip()

    if query == "q1":
        query1(edges_rdd)
    elif query == "q2":
        query2(edges_rdd)
    elif query == "q3":
        query3(edges_rdd, nodes_rdd)
    else:
        print("Error. Invalid query. Please enter q1, q2, or q3.")

    spark.stop()
