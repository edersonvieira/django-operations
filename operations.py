import json
import decimal
import logging
from typing import Any, Dict, Union, List, Optional
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from django.db import connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Union[float, int, Any]:
        if isinstance(o, decimal.Decimal):
            if o % 1:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

class OperationsDynamo:
    @staticmethod
    def get(tabela, params: Dict[str, Any]) -> Union[Dict[str, Any], bool]:
        try:
            response = tabela.get_item(Key=params)
        except ClientError as e:
            logging.error(f"Erro ao obter item: {e.response['Error']['Message']}")
            return False
        else:
            item = response.get('Item')
            if item:
                return json.loads(json.dumps(item, indent=4, cls=DecimalEncoder))
            logging.warning("Item não encontrado")
            return False

    @staticmethod
    def getByIndex(tabela, index: Dict[str, Any]) -> Union[List[Dict[str, Any]], bool]:
        index_key = list(index.keys())[0]
        try:
            response = tabela.query(
                IndexName=f"{index_key}-index",
                KeyConditionExpression=Key(index_key).eq(index[index_key])
            )
        except ClientError as e:
            logging.error(f"Erro ao consultar índice: {e.response['Error']['Message']}")
            return False
        else:
            items = response.get('Items')
            if items:
                return json.loads(json.dumps(items, indent=4, cls=DecimalEncoder))
            logging.warning("Itens não encontrados")
            return False

    @staticmethod
    def update(tabela, condition: Dict[str, Any], expression: str, attr: Dict[str, Any], attrnames: Optional[Dict[str, str]] = None) -> bool:
        try:
            update_args = {
                'Key': condition,
                'UpdateExpression': expression,
                'ExpressionAttributeValues': attr,
                'ReturnValues': "UPDATED_NEW"
            }
            if attrnames:
                update_args['ExpressionAttributeNames'] = attrnames

            response_update = tabela.update_item(**update_args)
        except ClientError as e:
            logging.error(f"Erro ao atualizar item: {e.response['Error']['Message']}")
            return False
        else:
            if 'Attributes' in response_update:
                return True
            logging.warning("Falha ao atualizar item")
            return False

    @staticmethod
    def create(tabela, params: Dict[str, Any]) -> Union[str, bool]:
        try:
            response = tabela.put_item(Item=params)
        except ClientError as e:
            logging.error(f"Erro ao criar item: {e.response['Error']['Message']}")
            return False
        else:
            return json.dumps(response, indent=4, cls=DecimalEncoder)

    @staticmethod
    def remove(tabela, params: Dict[str, Any], condition: str, attr: Dict[str, Any]) -> Union[str, bool]:
        try:
            response = tabela.delete_item(
                Key=params,
                ConditionExpression=condition,
                ExpressionAttributeValues=attr
            )
        except ClientError as e:
            logging.error(f"Erro ao remover item: {e.response['Error']['Message']}")
            return False
        else:
            return json.dumps(response, indent=4, cls=DecimalEncoder)

    @staticmethod
    def listAll(tabela, params: Any, fields: Optional[str], filter: Optional[Any], attrnames: Optional[Dict[str, str]] = None) -> Union[List[Dict[str, Any]], bool]:
        try:
            query = {
                'KeyConditionExpression': params,
                'ScanIndexForward': False
            }
            if fields:
                query['ProjectionExpression'] = fields
            if attrnames:
                query['ExpressionAttributeNames'] = attrnames
            if filter:
                query['FilterExpression'] = filter

            response = tabela.query(**query)
        except ClientError as e:
            logging.error(f"Erro ao listar todos os itens: {e.response['Error']['Message']}")
            return False
        else:
            items = response.get('Items')
            if items:
                return json.loads(json.dumps(items, indent=4, cls=DecimalEncoder))
            logging.warning("Nenhum item encontrado")
            return False

    @staticmethod
    def getFirstRegistry(tabela, params: Any, fields: Optional[str]) -> Union[List[Dict[str, Any]], bool]:
        try:
            query_args = {
                'KeyConditionExpression': params,
                'ScanIndexForward': True,
                'Limit': 1
            }
            if fields:
                query_args['ProjectionExpression'] = fields

            response = tabela.query(**query_args)
        except ClientError as e:
            logging.error(f"Erro ao obter primeiro registro: {e.response['Error']['Message']}")
            return False
        else:
            items = response.get('Items')
            if items:
                return json.loads(json.dumps(items, indent=4, cls=DecimalEncoder))
            logging.warning("Primeiro registro não encontrado")
            return False

    @staticmethod
    def getLastRegistry(tabela, params: Any, fields: Optional[str]) -> Union[List[Dict[str, Any]], bool]:
        try:
            query_args = {
                'KeyConditionExpression': params,
                'ScanIndexForward': False,
                'Limit': 1
            }
            if fields:
                query_args['ProjectionExpression'] = fields

            response = tabela.query(**query_args)
        except ClientError as e:
            logging.error(f"Erro ao obter último registro: {e.response['Error']['Message']}")
            return False
        else:
            items = response.get('Items')
            if items:
                return json.loads(json.dumps(items, indent=4, cls=DecimalEncoder))
            logging.warning("Último registro não encontrado")
            return False

    @staticmethod
    def listAllPaginate(tabela, params: Any, fields: Optional[str], attrnames: Optional[Dict[str, str]] = None) -> Union[List[Dict[str, Any]], bool]:
        try:
            query = {
                'KeyConditionExpression': params,
                'ScanIndexForward': False
            }
            if fields:
                query['ProjectionExpression'] = fields
            if attrnames:
                query['ExpressionAttributeNames'] = attrnames

            response = tabela.query(**query)
        except ClientError as e:
            logging.error(f"Erro ao listar todos os itens com paginação: {e.response['Error']['Message']}")
            return False
        else:
            items = response.get('Items')
            if items:
                data = items
                while 'LastEvaluatedKey' in response:
                    query['ExclusiveStartKey'] = response['LastEvaluatedKey']
                    response = tabela.query(**query)
                    data.extend(response.get('Items', []))
                return json.loads(json.dumps(data, indent=4, cls=DecimalEncoder))
            logging.warning("Nenhum item encontrado")
            return False

    @staticmethod
    def scanPaginate(tabela, expression: Any, fields: str, lastKey: Optional[Dict[str, Any]], limit: int = 0, attrnames: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        try:
            query = {
                'KeyConditionExpression': expression,
                'ScanIndexForward': False,
                'ProjectionExpression': fields,
                'Limit': limit
            }
            if attrnames:
                query['ExpressionAttributeNames'] = attrnames
            if lastKey:
                query['ExclusiveStartKey'] = lastKey

            response = tabela.query(**query)
        except ClientError as e:
            logging.error(f"Erro ao realizar scan com paginação: {e.response['Error']['Message']}")
            return {"lastKey": None, "results": []}
        else:
            items = response.get('Items')
            if items:
                return {
                    "lastKey": response.get('LastEvaluatedKey'),
                    "results": json.loads(json.dumps(items, indent=4, cls=DecimalEncoder)),
                }
            logging.warning("Nenhum item encontrado no scan")
            return {"lastKey": None, "results": []}

    @staticmethod
    def scanFilter(tabela, expression: Any, limit: int = 0) -> Union[List[Dict[str, Any]], bool]:
        try:
            response = tabela.query(
                KeyConditionExpression=expression,
                Limit=limit,
                ScanIndexForward=False
            )
        except ClientError as e:
            logging.error(f"Erro ao realizar scan com filtro: {e.response['Error']['Message']}")
            return False
        else:
            items = response.get('Items')
            if items:
                return json.loads(json.dumps(items, indent=4, cls=DecimalEncoder))
            logging.warning("Nenhum item encontrado no scan com filtro")
            return False

    @staticmethod
    def queryFilter(tabela, expression: Any, filtro: Optional[Dict[str, Any]]) -> Union[List[Dict[str, Any]], bool]:
        try:
            if filtro:
                response = tabela.query(
                    KeyConditionExpression=expression,
                    FilterExpression="tela = :t OR descricao = :d OR cliente = :c",
                    ExpressionAttributeValues={":t": filtro['tela'], ":d": False, ":c": False}
                )
            else:
                response = tabela.query(KeyConditionExpression=expression)
        except ClientError as e:
            logging.error(f"Erro ao realizar query com filtro: {e.response['Error']['Message']}")
            return False
        else:
            items = response.get('Items')
            if items:
                return json.loads(json.dumps(items, indent=4, cls=DecimalEncoder))
            logging.warning("Nenhum item encontrado na query com filtro")
            return False

    @staticmethod
    def countQuery(tabela, params: Any) -> Union[int, bool]:
        try:
            response = tabela.query(
                KeyConditionExpression=params,
                ScanIndexForward=False,
                Select='COUNT'
            )
        except ClientError as e:
            logging.error(f"Erro ao realizar count query: {e.response['Error']['Message']}")
            return False
        else:
            count = response.get('Count')
            if count is not None:
                return count
            logging.warning("Nenhum item encontrado na count query")
            return False
        
class OperationsSQL:
    @staticmethod
    def get(tabela: str, condicao: str) -> Optional[Dict[str, Any]]:
        query = f"SELECT * FROM {tabela} WHERE {condicao} LIMIT 1"

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
                if row:
                    columns = [col[0] for col in cursor.description]
                    return dict(zip(columns, row))
                logging.warning("Nenhum registro encontrado na consulta 'get'")
                return None
        except Exception as e:
            logging.error(f"Erro ao executar a consulta 'get': {e}")
            return None
    
    @staticmethod
    def filter(tabela: str, condicao: Optional[str] = None, campo_ordenar: Optional[str] = None, ordenacao: Optional[str] = None, limite_por_pagina: int = 5, pagina: int = 1, inner_join_receive: Optional[str] = None, select_receive: Optional[str] = None) -> List[Dict[str, Any]]:
        offset = (pagina - 1) * limite_por_pagina
        
        select = '*'
        inner_join = ""
        order_clause = ""
        where_clause = f"WHERE {condicao}" if condicao else ""
        if campo_ordenar and ordenacao:
            order_clause = f"ORDER BY {campo_ordenar} {ordenacao}"
        if inner_join_receive:
            inner_join = inner_join_receive
        if select_receive:
            select = select_receive
        
        query = f"""
            SELECT {select} FROM {tabela}
            {inner_join}
            {where_clause}
            {order_clause}
            LIMIT {limite_por_pagina} OFFSET {offset}
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                results = [
                    dict(zip(columns, row))
                    for row in cursor.fetchall()
                ]
            return results
        except Exception as e:
            logging.error(f"Erro ao executar a consulta 'filter': {e}")
            return []
        
    @staticmethod
    def count(tabela: str, field_count: str, condicao: Optional[str] = None, inner_join_receive: Optional[str] = None) -> List[Dict[str, Any]]:
        select = 'COUNT(' + field_count + ') AS total'
        inner_join = ""
        where_clause = f"WHERE {condicao}" if condicao else ""
        if inner_join_receive:
            inner_join = inner_join_receive
        
        query = f"""
            SELECT {select} FROM {tabela}
            {inner_join}
            {where_clause}
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                columns = [col[0] for col in cursor.description]
                results = [
                    dict(zip(columns, row))
                    for row in cursor.fetchall()
                ]
            return results
        except Exception as e:
            logging.error(f"Erro ao executar a consulta 'count': {e}")
            return []
