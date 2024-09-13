from pubchempy import get_compounds, Compound

comp = Compound.from_cid(1423)
print(comp.isomeric_smiles)
