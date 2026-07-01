# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aabf914c-0501-4c58-ba5b-4b0f05f4420f",
# META       "default_lakehouse_name": "SILVER",
# META       "default_lakehouse_workspace_id": "c8d75176-b949-4f7e-a658-b996603ec8c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "aabf914c-0501-4c58-ba5b-4b0f05f4420f"
# META         },
# META         {
# META           "id": "5db3d583-e11f-4ac4-9781-65ee3ee820a0"
# META         },
# META         {
# META           "id": "45614522-af60-4d44-aeeb-f66a7da63b13"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Track Machine Learning experiments and models
# 
# A machine learning model is a file that has been trained to recognize certain types of patterns. You train a model over a set of data, providing it an algorithm that it can use to reason over and learn from those data. Once you have trained the model, you can use it to reason over data that it hasn't seen before, and make predictions about that data.
# 
# In this notebook, you will learn the basic steps to run an experiment, add a model version to track run metrics and parameters and register a model.


# CELL ********************

import mlflow

# Set given experiment as the active experiment. If an experiment with this name does not exist, a new experiment with this name is created.
mlflow.set_experiment("Monitor")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow.sklearn
import numpy as np
from sklearn.linear_model import LogisticRegression
from mlflow.models.signature import infer_signature

# Start your training job with `start_run()`
with mlflow.start_run() as run:

    lr = LogisticRegression()
    X = np.array([-2, -1, 0, 1, 2, 1]).reshape(-1, 1)
    y = np.array([0, 0, 1, 1, 1, 0])
    lr.fit(X, y)
    score = lr.score(X, y)
    signature = infer_signature(X, y)

    # Activate the MLFlow logging API to log your training job metrics
    print("test log_metrics.")
    mlflow.log_metric("score", score)

    # Activate the MLFlow logging API to log your training job parameters
    print("test log_params.")
    mlflow.log_param("alpha", "alpha")
    print("All done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
