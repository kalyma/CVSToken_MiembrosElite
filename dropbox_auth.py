# dropbox_auth.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()

class DropboxAuth:
    """
    Clase para manejar la autenticación con Dropbox API.
    Versión estable y probada - Úsala exactamente así.
    """
    
    @staticmethod
    def renovar_access_token():
        """
        Renueva el token de acceso usando el refresh token.
        Devuelve:
            - access_token (str) si es exitoso
            - None si falla
        """
        try:
            # 1. Obtener credenciales del entorno
            refresh_token = os.getenv('DROPBOX_REFRESH_TOKEN')
            app_key = os.getenv('DROPBOX_APP_KEY')
            app_secret = os.getenv('DROPBOX_APP_SECRET')
            
            if not all([refresh_token, app_key, app_secret]):
                print("❌ Faltan credenciales en el archivo .env")
                return None

            # 2. Hacer la petición a la API de Dropbox
            response = requests.post(
                'https://api.dropbox.com/oauth2/token',
                data={
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token
                },
                auth=(app_key, app_secret),
                timeout=10  # Timeout para evitar esperas infinitas
            )
            
            # 3. Verificar si la respuesta es exitosa
            response.raise_for_status()
            
            # 4. Extraer y devolver el access token
            access_token = response.json().get('access_token')
            print("✅ Token renovado correctamente")
            return access_token
            
        except requests.exceptions.RequestException as e:
            # Manejo detallado de errores
            error_msg = f"❌ Error renovando token: {str(e)}"
            if hasattr(e, 'response') and e.response:
                error_msg += f"\nDetalle del error: {e.response.text}"
            print(error_msg)
            return None
        except Exception as e:
            print(f"❌ Error inesperado: {str(e)}")
            return None