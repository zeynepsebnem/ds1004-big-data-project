import pandas as pd
from scipy.stats import pearsonr

df = pd.read_csv("parkslope_bedstuy_sale_burglary.csv", sep=",", header = 0)

bedstuy = df[df['hood'] == 'bedford stuyvesant']
parkslope = df[df['hood'] == 'park slope']

bedstuy_pearson = pearsonr(bedstuy['avg price'], bedstuy['count_felony'])
parkslope_pearson = pearsonr(parkslope['avg price'], parkslope['count_felony'])

print("bedstuy pearson: "+str(bedstuy_pearson))
print("park slope pearson: "+str(parkslope_pearson))