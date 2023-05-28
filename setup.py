from google.colab import auth 
auth.authenticate_user()

from google.colab import drive
drive.mount('/content/drive')

!pip install apache-beam[gcp]
!pip install pyspark