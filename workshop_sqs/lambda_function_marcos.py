import json
import boto3
import pandas as pd 
from io import StringIO
from datetime import datetime


s3_client = boto3.client('s3')


def lambda_handler(event, context):
    for e in event["Records"]:
        messages = json.loads(e['body'])
        
        # Validar se a mensagem tem Records (evento S3 válido)
        if "Records" not in messages:
            print(f"Mensagem sem Records, pulando: {messages}")
            continue
            
        for record in messages["Records"]:
            if 's3' in record:
                s3_bucket = record['s3']['bucket']['name']
                s3_key = record['s3']['object']['key']

                print(f"Bucket: {s3_bucket}")
                print(f"Key: {s3_key}")

                try:
                    s3_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                    
                    # Tentar múltiplas codificações
                    file_content = None
                    for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                        try:
                            file_content = s3_object["Body"].read().decode(encoding)
                            print(f"Arquivo decodificado com sucesso usando: {encoding}")
                            break
                        except UnicodeDecodeError:
                            s3_object["Body"] = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)["Body"]
                            continue
                    
                    if file_content is None:
                        raise Exception("Não foi possível decodificar o arquivo com nenhuma codificação testada")

                    df = pd.read_csv(StringIO(file_content))
                    df['nome_arquivo'] = s3_key
                    df['data_hora'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    print(f"DataFrame processado com {len(df)} linhas")
                    print(df.head())
                    
                except Exception as ex:
                    print(f"Erro ao processar arquivo {s3_key}: {str(ex)}")
                    raise

                output_buffer = StringIO()
                df.to_csv(output_buffer, index=False)
                output_content = output_buffer.getvalue()

                out_key = f"silver/{s3_key.replace('bronze/','')}"
                s3_client.put_object(Bucket=s3_bucket, Key=out_key, Body=output_content)
            else:
                print('Error parsing message')
    return {
        'statusCode': 200,
        'body': json.dumps('Processamento concluído com sucesso!')
    }
