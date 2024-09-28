# Databricks notebook source
# DBTITLE 1,Read The File
import pandas as pd

file_path = '../files/customer.csv'
df = pd.read_csv(file_path)
df.display()
