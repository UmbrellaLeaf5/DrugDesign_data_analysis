import pubchempy as pcp

comp = pcp.Compound.from_cid(1423)
print(comp.to_dict())
# pcp.get_compounds("molecular_weight=5")
