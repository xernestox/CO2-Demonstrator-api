from unicodedata import decimal
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from numpy import float64
import pandas as pd


app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    
    file_table = pd.read_csv('Dataset DataApp.csv')
    file_dataFrame = pd.DataFrame(file_table)
    file_dataFrame = file_dataFrame.round(decimals=2)

    #Sacar columnas de emisiones y nombre del proceso 

    emissions = file_dataFrame[['Section', 'CO2e Emissions (kg) per Tyre', 'Type']]

    #Sacar emisiones totales

    total_emissions = emissions[['CO2e Emissions (kg) per Tyre']].sum(axis = 0, numeric_only=1)
    total_emissions = total_emissions.round(decimals=2)

    # #Sacar porcentajes de emissiones y nombre del proceso

    emissions_pct = pd.DataFrame(columns=['Section','CO2e Emissions (%) per Tyre'])
    emissions_pct['Section'] = emissions.loc[:,'Section']
    emissions_pct['CO2e Emissions (%) per Tyre'] = emissions.loc[:,'CO2e Emissions (kg) per Tyre']/float64(total_emissions)*100
    emissions_pct = emissions_pct.round(decimals=1)

    #Ordenar secciones por lugar de emisiones

    emissions_sorted_as = emissions.sort_values(by=['CO2e Emissions (kg) per Tyre'],ascending=True)
    emissions_sorted_ds = emissions.sort_values(by=['CO2e Emissions (kg) per Tyre'],ascending=False)

    #Suma de emisiones por materiales y por procesos

    material_emissions = file_dataFrame[['Section', 'CO2e Emissions (kg) per Tyre']].loc[file_dataFrame['Type'] == 'Material']
    process_emissions = file_dataFrame[['Section', 'CO2e Emissions (kg) per Tyre']].loc[file_dataFrame['Type'] == 'Process']

    total_material_emissions = material_emissions[['CO2e Emissions (kg) per Tyre']].sum(axis = 0, numeric_only=1)
    total_material_emissions = total_material_emissions.round(decimals=2)
    max_material = material_emissions.loc[material_emissions['CO2e Emissions (kg) per Tyre'].idxmax()]

    
    total_process_emissions = process_emissions[['CO2e Emissions (kg) per Tyre']].sum(axis = 0, numeric_only=1)
    total_process_emissions = total_process_emissions.round(decimals=2)
    max_process = process_emissions.loc[process_emissions['CO2e Emissions (kg) per Tyre'].idxmax()]


    co2data = {
    "file_dataFrame": file_dataFrame,
    "emissions": emissions,
    "total_emissions": total_emissions,
    "emissions_pct": emissions_pct,
    "emissions_sorted_as": emissions_sorted_as,
    "emissions_sorted_ds": emissions_sorted_ds,
    "material_emissions": material_emissions,
    "process_emissions": process_emissions,
    "total_material_emissions": total_material_emissions,
    "total_process_emissions": total_process_emissions,
    "max_material": max_material,
    "max_process": max_process,
    }

    return co2data

@app.get("/connected-datasets")
async def connecteddatasets():

    file_table = pd.read_csv('Dataset DataApp ConnectedDatasets.csv')
    file_dataFrame = pd.DataFrame(file_table)

    connectedDatasets = {
    "file_dataFrame": file_dataFrame,
    }
    
    return connectedDatasets

@app.get("/oem")
async def oem():

    file_table = pd.read_csv('Dataset DataApp OEM.csv')
    file_dataFrame = pd.DataFrame(file_table)
    file_dataFrame = file_dataFrame.round(decimals=2)

    oemco2data = {
    "file_dataFrame": file_dataFrame,
    }
    
    return oemco2data

@app.get("/tier1")
async def tier1():

    file_table = pd.read_csv('Dataset DataApp Tier 1.csv')
    file_dataFrame = pd.DataFrame(file_table)
    file_dataFrame = file_dataFrame.round(decimals=2)

    tier1co2data = {
    "file_dataFrame": file_dataFrame,
    }
    
    return tier1co2data

@app.get("/tier2")
async def tier2():

    file_table = pd.read_csv('Dataset DataApp Tier 2.csv')
    file_dataFrame = pd.DataFrame(file_table)
    file_dataFrame = file_dataFrame.round(decimals=2)


    tier2co2data = {
    "file_dataFrame": file_dataFrame,
    }
    
    return tier2co2data
