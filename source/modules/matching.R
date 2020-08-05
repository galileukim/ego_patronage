

# 1) start at year t.
# 2) filter out employees for year t. (create a dictionary)
# 3) look up all party members who had a valid membership for year t.
# 4) perform exact matching on (kmer) name using employee_t & party_member_t. (do it as few times as possible)
# 5) do a hash on the first blocks.
# 6) deidentify data, cryptograph
# 7) probabilistic matching on the remainder using either fastLink or cycle through with k-mer. (do surgery to extract the code that you need)
# 7) levenstein (edit distance): and do an eyeballing.
# 8) repeat for all years in T.