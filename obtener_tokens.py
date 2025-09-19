# obtener_tokens.py
import requests
from dotenv import load_dotenv
import os
import webbrowser
from urllib.parse import quote

load_dotenv()

def generar_url_autorizacion():
    """Genera la URL para obtener el código de autorización"""
    app_key = os.getenv('DROPBOX_APP_KEY')
    if not app_key:
        print("❌ Faltan DROPBOX_APP_KEY en .env")
        return None
    
    redirect_uri_encoded = quote("https://localhost", safe='')
    
    url = (f"https://www.dropbox.com/oauth2/authorize?"
           f"client_id={app_key}&"
           f"token_access_type=offline&"
           f"response_type=code&"
           f"redirect_uri={redirect_uri_encoded}")  # ¡IMPORTANTE!
    return url

def obtener_tokens():
    # 1. Mostrar URL para obtener el código
    auth_url = generar_url_autorizacion()
    if not auth_url:
        return
        
    print("\n1. Abre este enlace en tu navegador:")
    print(auth_url)
    webbrowser.open(auth_url)
    
    # 2. Pedir el código manualmente
    auth_code = input("\n2. Después de autorizar, pega aquí el código de la URL: ").strip()
    
    # 3. Obtener los tokens
    try:
        response = requests.post(
            "https://api.dropbox.com/oauth2/token",
            data={
                "code": auth_code,
                "grant_type": "authorization_code",
                "client_id": os.getenv('DROPBOX_APP_KEY'),
                "client_secret": os.getenv('DROPBOX_APP_SECRET'),
                "redirect_uri": "https://localhost"  # IMPORTANTE
            },
            timeout=10
        )
        
        if response.status_code != 200:
            print(f"\n❌ Error de Dropbox (HTTP {response.status_code}):")
            print(response.text)
            print(f"\n📋 Datos enviados:")
            print(f"code: {auth_code}")
            print(f"client_id: {os.getenv('DROPBOX_APP_KEY')}")
            print(f"client_secret: {os.getenv('DROPBOX_APP_SECRET')}")
            print(f"redirect_uri: https://localhost")
            return
            
        tokens = response.json()
        
        if 'access_token' not in tokens or 'refresh_token' not in tokens:
            print("\n⚠️ Respuesta inesperada de Dropbox:")
            print(tokens)
            return
            
        print("\n✅ Tokens obtenidos correctamente!")
        print("\nAgrega estas líneas a tu archivo .env:\n")
        print(f"DROPBOX_ACCESS_TOKEN={tokens['access_token']}")
        print(f"DROPBOX_REFRESH_TOKEN={tokens['refresh_token']}\n")
        
    except Exception as e:
        print(f"\n❌ Error al obtener tokens: {str(e)}")

if __name__ == "__main__":
    obtener_tokens()