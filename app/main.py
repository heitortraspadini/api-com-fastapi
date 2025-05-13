from fastapi import FastAPI, HTTPException
from typing import Type
from model.db import Database
from model.models import Serie, Ator, Motivo, Avaliacao, Categoria, BaseModel  # Corrigido o import para "models"

app = FastAPI()
db = Database()

def executar_operacao_db(sql: str, params: tuple = None):
    db.conectar()
    db.executar_comando(sql, params)
    db.desconectar()

def executar_consulta_db(sql: str, params: tuple = None):
    db.conectar()
    resultados = db.executar_comando(sql, params)
    db.desconectar()
    return resultados

def rota_post(app: FastAPI, path: str, model: Type[BaseModel], table_name: str):
    @app.post(path)
    def criar_item(item: model):
        campos = ", ".join(item.dict().keys())
        valores = ", ".join(["%s"] * len(item.dict()))
        sql = f"INSERT INTO {table_name} ({campos}) VALUES ({valores})"
        executar_operacao_db(sql, tuple(item.dict().values()))
        return {"saída": f"{table_name.capitalize()} cadastrado com sucesso"}
    return criar_item

def rota_get(app: FastAPI, path: str, table_name: str):
    @app.get(path)
    def listar_itens():
        sql = f"SELECT * FROM {table_name}"
        return executar_consulta_db(sql)
    return listar_itens

def rota_update(app: FastAPI, path: str, model: Type[BaseModel], table_name: str, id_field: str):
    @app.put(path)
    def atualizar_item(item_id: int, item: model):
        updates = ", ".join([f"{key} = %s" for key in item.dict().keys()])
        sql = f"UPDATE {table_name} SET {updates} WHERE {id_field} = %s"
        executar_operacao_db(sql, tuple(list(item.dict().values()) + [item_id]))
        return {"saída": f"{table_name.capitalize()} atualizado com sucesso"}
    return atualizar_item

def rota_delete(app: FastAPI, path: str, table_name: str, id_field: str):
    @app.delete(path)
    def deletar_item(item_id: int):
        sql = f"DELETE FROM {table_name} WHERE {id_field} = %s"
        executar_operacao_db(sql, (item_id,))
        return {"saída": f"{table_name.capitalize()} deletado com sucesso"}
    return deletar_item

# Aplicação das rotas genéricas
adicionar_serie = rota_post(app, "/series/", Serie, "serie")
listar_series = rota_get(app, "/series/", "serie")
atualizar_serie = rota_update(app, "/update_serie/{id}", Serie, "serie", "id") 
deletar_serie = rota_delete(app, "/delete_serie/{id}", "serie", "id") 

adicionar_autor = rota_post(app, "/atores/", Ator, "ator")  
listar_atores = rota_get(app, "/atores/", "ator")  
atualizar_autor = rota_update(app, "/update_ator/{id}", Ator, "ator", "id")  
deletar_autor = rota_delete(app, "/delete_ator/{id}", "ator", "id")  

adicionar_categoria = rota_post(app, "/categorias/", Categoria, "categoria")
listar_categorias = rota_get(app, "/categorias/", "categoria")
atualizar_categoria = rota_update(app, "/update_categoria/{id}", Categoria, "categoria", "id")  
deletar_categoria = rota_delete(app, "/delete_categoria/{id}", "categoria", "id")  

adicionar_motivo_pessoal = rota_post(app, "/motivos/", Motivo, "motivo_assistir")
listar_motivos = rota_get(app, "/motivos/", "motivo_assistir")
atualizar_motivo = rota_update(app, "/update_motivo/{id}", Motivo, "motivo_assistir", "id")  
deletar_motivo = rota_delete(app, "/delete_motivo/{id}", "motivo_assistir", "id")  

adicionar_avaliacao = rota_post(app, "/avaliacoes/", Avaliacao, "avaliacao_serie")
listar_avaliacoes = rota_get(app, "/avaliacoes/", "avaliacao_serie")
atualizar_avaliacao = rota_update(app, "/update_avaliacao/{id}", Avaliacao, "avaliacao_serie", "id")  
deletar_avaliacao = rota_delete(app, "/delete_avaliacao/{id}", "avaliacao_serie", "id")  

# Rotas de associação (estas são mais específicas)
@app.get("/ator/{id}/series/")
def listar_serie_e_ator_relacionados(id: int):
    sql = """
        SELECT s.id, s.titulo, s.descricao, s.ano_lancamento
        FROM serie s
        JOIN ator_serie as ON s.id = as.id_serie
        WHERE as.id_ator = %s
    """
    return executar_consulta_db(sql, (id,))

@app.post("/ator/{id_ator}/series/{id_serie}")
def associar_ator_serie(id_ator: int, id_serie: int):
    
    sql_ator = "SELECT COUNT(*) FROM ator WHERE id = %s"
    ator_count = executar_consulta_db(sql_ator, (id_ator,))
    if ator_count[0]['COUNT(*)'] == 0:
        raise HTTPException(status_code=404, detail="Ator não encontrado")
    
    sql_serie = "SELECT COUNT(*) FROM serie WHERE id = %s"
    serie_count = executar_consulta_db(sql_serie, (id_serie,))
    if serie_count[0]['COUNT(*)'] == 0:
        raise HTTPException(status_code=404, detail="Série não encontrada")
    
    sql_insert = "INSERT INTO ator_serie (id_ator, id_serie) VALUES (%s, %s)"
    executar_operacao_db(sql_insert, (id_ator, id_serie))
    return {"saída": "Ator associado à série com sucesso"}