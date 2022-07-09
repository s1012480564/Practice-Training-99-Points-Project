import pandas as pd
import sklearn2pmml as pmml
from sklearn2pmml import PMMLPipeline
from sklearn.ensemble import BaggingRegressor

def save_model(data, model_path):
    x=data.iloc[:,0].values.reshape([len(data),1])
    y=data.iloc[:,1]
    pipeline=PMMLPipeline([("bagging", BaggingRegressor())])
    pipeline.fit(x,y)
    pmml.sklearn2pmml(pipeline,model_path,with_repr=True)

if __name__ == "__main__":
    data=pd.read_csv("income.csv",header=None)
    save_model(data,"income_model.pmml")
    print("模型保存完成。")