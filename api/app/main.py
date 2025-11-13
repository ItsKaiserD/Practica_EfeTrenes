from fastapi import FastAPI

# Instancia principal de la app
app = FastAPI(title="MCP API - Practica EFE Trenes")


# Endpoint simple para probar que todo funciona
@app.get("/health")
def health():
    return {"status": "ok", "message": "API MCP funcionando"}


# Aquí después iremos agregando los endpoints reales de ingesta MCP
# por ejemplo /ingesta/indicadores, etc.
# Por ahora dejamos solo el endpoint de salud