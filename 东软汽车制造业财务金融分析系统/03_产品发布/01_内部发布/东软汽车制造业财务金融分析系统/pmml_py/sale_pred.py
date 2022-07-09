import pandas as pd
import sklearn2pmml as pmml
from sklearn2pmml import PMMLPipeline
from sklearn.ensemble import BaggingRegressor

def save_model(data, model_path):
    X=data.iloc[:,0:-1]
    y=data.iloc[:,-1]
    pipeline=PMMLPipeline([("bagging", BaggingRegressor())])
    pipeline.fit(X,y)
    pmml.sklearn2pmml(pipeline,model_path,with_repr=True)

if __name__ == "__main__":
    data=pd.read_csv("sale.csv",header=None)
    save_model(data,"sale_model.pmml")
    print("模型保存完成。")