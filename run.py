"""
Ponto de entrada para rodar o site de vendas.
Execute: python run.py
Acesse:  http://localhost:5000
Admin:   http://localhost:5000/admin  (senha: admin123)
"""
from app import app

if __name__ == "__main__":
    print("\n" + "="*50)
    print("  ⚡ BotStore — Site de Vendas")
    print("="*50)
    print("  🌐 Loja:  http://localhost:5000")
    print("  🔒 Admin: http://localhost:5000/admin")
    print("  🔑 Senha padrão: admin123")
    print("="*50 + "\n")
    app.run(debug=True, port=5000, host="0.0.0.0")
