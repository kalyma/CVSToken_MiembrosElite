# cronjob.py
import csv
import math
import os
import sys
import time
import json
import dropbox
import logging
import requests
import psycopg2
import shutil
import tempfile
from dotenv import load_dotenv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, InvalidSessionIdException, SessionNotCreatedException

load_dotenv()

class SkoolScraper:
    URLS = {
        'login': 'https://www.skool.com/login',
        'members': 'https://www.skool.com/mentoriavipantoecom/-/members'
    }
    SELECTORS = {
        'member_item': '[class*="styled__MemberItemWrapper-"]',
        'email_input': '#email',
        'password_input': '#password'
    }
    XPATHS = {
        'submit_button': '//button[@type="submit"]',
        'next_button': '//button[.//span[contains(text(), "Next")]]'
    }

    def _check_driver_alive(self):
        try:
            self.driver.current_url
            return True
        except:
            return False

    def __init__(self):
        self.max_pages_per_session = 10  # Reiniciar cada 5 páginas
        self.current_session_pages = 0
        self.script_name = os.path.basename(sys.argv[0])
        self.logger = self._iniciar_logger()
        self.credentials = self._cargar_credenciales()
        self.driver = None # Se inicializará en run()
        self.global_count = 0
        self.num_members = self._cargar_num_members()
        self._setup_database_connection()
        self.header_csv = [
            "Pag", "Cons_Pag", "Cons_Mbro", "Miembro_SK", "Nivel_SK", "Email_Gmail", 
            "Ult_Ingreso", "Fec_Unido", "Valor", "Contribucion", "Renueva", "Usuario_SK",
            "Frase", "Localizado", "Invito", "Dias", "Meses", "Total_Cursos", "Progreso_Total", "Porcentaje_Promedio"
        ]
        for i in range(1, 30):
            self.header_csv.extend([f"Curso_{i}_Nombre", f"Curso_{i}_Avance", f"Curso_{i}_Progreso"])

    def _reiniciar_navegador(self):
        """Reinicia el navegador para evitar problemas de memoria"""
        try:
            if self.driver:
                self.driver.quit()
                self.logger.info("🔄 Cerrando navegador para reinicio...")
        except:
            pass
        
        # Limpiar directorio temporal
        if hasattr(self, 'user_data_dir') and os.path.exists(self.user_data_dir):
            try:
                shutil.rmtree(self.user_data_dir, ignore_errors=True)
            except:
                pass
        
        # Esperar antes de reiniciar
        time.sleep(2)
        return self._iniciar_driver()

    def _iniciar_logger(self):
        # ... (sin cambios)
        logger = logging.getLogger("SkoolScraper")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("[%(levelname)s] %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def _cargar_credenciales(self):
        # ... (sin cambios)
        creds = {'email': os.getenv('SKOOL_EMAIL'), 'password': os.getenv('SKOOL_PASSWORD')}
        if not all(creds.values()): raise ValueError("Faltan credenciales de Skool en .env")
        return creds

    def _iniciar_driver(self):
        self.logger.info("🚗 Iniciando el navegador Selenium...")
        options = Options()
        
        # --- Para depurar, ejecuta en modo normal (con interfaz gráfica) ---
        # Comenta la siguiente línea para ver qué hace el navegador. ¡Es el mejor primer paso!
        options.add_argument("--headless=new") 
        # --------------------------------------------------------------------

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu") # A menudo útil en headless
        options.add_argument("--enable-unsafe-swiftshader")
        options.add_argument('--disable-logging')
        options.add_argument('--log-level=3')
        
        # Mantenemos las opciones clave para evitar la detección
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Directorio temporal para no mezclar sesiones
        import tempfile
        self.user_data_dir = tempfile.mkdtemp(prefix=f"chrome_{int(time.time())}_")
        options.add_argument(f"--user-data-dir={self.user_data_dir}")

        try:
            os.system("pkill -f chrome")
            os.system("pkill -f chromedriver")
            time.sleep(2)
        except:
            pass


        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.driver = webdriver.Chrome(options=options)
                # Aplicamos el script anti-detección DESPUÉS de iniciar el driver
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.logger.info("✅ Navegador iniciado correctamente.")
                return True
            except SessionNotCreatedException as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    self.logger.warning(f"⚠️ Intento {attempt + 1}/{max_retries} fallido. Esperando {wait_time} segundos...")
                    try:
                        if self.driver:
                            self.driver.quit()
                    except:
                        pass
                    try:
                        os.system("pkill -f chrome")
                        os.system("pkill -f chromedriver")
                    except:
                        pass
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"❌ No se pudo iniciar el navegador después de {max_retries} intentos: {e}")
                    return False
            except Exception as e:
                self.logger.error(f"❌ Error inesperado al iniciar el navegador: {e}")
                return False
            

    def extract_time(text):
        return text.split()[1] 
    

    def login(self):
        self.logger.info("🔐 Iniciando sesión en Skool...")
        try:
            self.driver.get(self.URLS['login'])
            wait = WebDriverWait(self.driver, 20) # Aumentamos a 20s por si la red es lenta
            
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS['email_input']))).send_keys(self.credentials['email'])
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS['password_input']))).send_keys(self.credentials['password'])
            wait.until(EC.element_to_be_clickable((By.XPATH, self.XPATHS['submit_button']))).click()
            
            # --- MEJORA ---
            # En lugar de esperar a que la URL cambie, esperamos a un elemento que SOLO existe
            # después de un login exitoso. Por ejemplo, el contenedor de los miembros.
            # Esto es mucho más fiable.
            self.logger.info("...esperando confirmación de login (carga de la página principal)...")
            #wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS['member_item'])))
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[class*="styled__GroupLogoWrapper"]')))
            
            self.logger.info("✅ Login exitoso.")
            return True
        except TimeoutException:
            self.logger.error("❌ Timeout durante el login. La página no cargó a tiempo o las credenciales son incorrectas.")
            # Opcional: tomar una captura de pantalla para ver qué salió mal
            # self.driver.save_screenshot('login_error.png')
            return False
        except Exception as e:
            self.logger.error(f"❌ Error inesperado durante el login: {e}", exc_info=True)
            return False
        
    def _cargar_num_members(self):
        # ... (sin cambios)
        try: return int(os.getenv('NUM_MEMBERS', 0))
        except ValueError: return 0
        
    def _setup_database_connection(self):
        # ... (sin cambios)
        self.connection_string = os.getenv('DATABASE_URL')
        try:
            conn = psycopg2.connect(self.connection_string)
            conn.close()
            self.logger.info("Conexión a PostgreSQL configurada correctamente")
        except Exception as e:
            self.logger.error(f"Error al conectar a PostgreSQL: {e}")

    
    def _generar_token_dropbox(self):
        self.logger.info("🔄 Generando nuevo token de acceso de Dropbox...")
        data = {"grant_type": "refresh_token", "refresh_token": os.getenv('DROPBOX_REFRESH_TOKEN')}
        try:
            response = requests.post("https://api.dropboxapi.com/oauth2/token", data=data, auth=(os.getenv('DROPBOX_APP_KEY'), os.getenv('DROPBOX_APP_SECRET')))
            response.raise_for_status()
            self.logger.info("🔑 Nuevo token de acceso generado correctamente.")
            return response.json().get("access_token")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ Error de red al renovar el token: {e.response.text if e.response else e}")
            return None

    def subir_a_dropbox(self, nombre_archivo):
        access_token = self._generar_token_dropbox()
        if not access_token: return
        if not os.path.exists(nombre_archivo):
            self.logger.error(f"🚫 Archivo local no encontrado: {nombre_archivo}")
            return
        
        self.logger.info(f"☁️ Iniciando subida de '{nombre_archivo}' a Dropbox...")
        dbx = dropbox.Dropbox(access_token)
        dropbox_path = f"/{os.path.basename(nombre_archivo)}"
        try:
            with open(nombre_archivo, "rb") as f:
                dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            self.logger.info("✅ Subida a Dropbox completada exitosamente.")
        except Exception as e:
            self.logger.error(f"❌ Error crítico al subir a Dropbox: {e}", exc_info=True)

    def _log_member_structure(self, nombre_miembro, parts, raw_text):
        """Registra la estructura de un miembro para seguimiento"""
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'miembro': nombre_miembro,
                'raw_text': raw_text,
                'parts_structure': parts,
                'parts_count': len(parts),
                'parts_detail': {f'part_{i}': part for i, part in enumerate(parts)}
            }
            
            # Guardar en archivo JSON (mejor para análisis posterior)
            log_file = "member_structure_log.json"
            
            # Si el archivo existe, cargamos y añadimos
            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            else:
                existing_data = []
            
            existing_data.append(log_entry)
            
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=2, ensure_ascii=False)
                
            # También registrar en el logger estándar
            self.logger.info(f"Estructura miembro monitoreado - {nombre_miembro}:")
            self.logger.info(f"Raw text: {raw_text}")
            self.logger.info(f"Parts ({len(parts)}): {parts}")
            
        except Exception as e:
            self.logger.error(f"Error registrando estructura miembro: {str(e)}")
   
    def _extraer_info_miembro(self, miembro_element):
        data = {'Nivel': 'N/A', 'Miembro': 'N/A', 'EmailSkool': 'N/A', 'Chat': 'N/A', 'Membership': 'N/A', 'Frase': 'N/A', 'Activo': 'N/A', 'Unido': 'N/A', 'Valor': 'N/A', 'Renueva': 'N/A', 'Localiza': 'N/A', 'Invito': 'N/A', 'Invitado': 'N/A', 'Otro': 'N/A'}
        try:
            #miembros_monitoreo = [                "Ramiro Oliviere"            ]
            parts = [p.strip() for p in miembro_element.text.split('\n') if p.strip()]
            keys = list(data.keys())
            for i, part in enumerate(parts):
                if i < len(keys): data[keys[i]] = part

            #if data['Miembro'] in miembros_monitoreo:
                #self._log_member_structure(data['Miembro'], parts, miembro_element.text)

            #if data['Miembro'] == 'Helen Mishel':
                #print("Debug aquí")

            if data['EmailSkool'].startswith('(Admin)'):
                data['Otro'] = data['EmailSkool']
                data['EmailSkool'] = data['Chat']
                data['Chat'] = data['Membership']
                data['Membership'] = data['Frase']
                data['Frase'] = data['Activo']
                data['Activo'] = data['Unido']
                data['Unido'] = data['Valor']
                data['Valor'] = data['Renueva']
                data['Renueva'] = data['Localiza']
                data['Localiza'] = data['Invito']
                data['Invito'] = data['Otro']

            if data['Frase'].startswith('Active ') or data['Frase'] == '':
                data['Otro'], data['Renueva'], data['Valor'], data['Unido'], data['Activo'], data['Frase'] = data['Renueva'], data['Valor'], data['Unido'], data['Activo'], data['Frase'], 'N/A'
            
            if not (data['Valor'].startswith('$') or data['Valor'].startswith('Free')):
                data['Otro'] = data['Valor']
                data['Valor'] = data['Renueva']
                data['Renueva'] = data['Localiza']
                data['Localiza'] = data['Otro']
            
            valor_str = data.get('Valor', '')
            localiza_str = data.get('Localiza', '')
            condition1 = not valor_str.startswith('$')
            condition2 = not valor_str.startswith('Free')
            condition3 = localiza_str.startswith('Renews')
            if (condition1 or condition2) and condition3:
                data['Otro'], data['Valor'], data['Renueva'], data['Localiza'] = data['Valor'], data['Renueva'], data['Localiza'], data['Otro']
            if data['Localiza'].startswith('Renews'):
                data['Otro'], data['Localiza'], data['Valor'], data['Renueva'] = data['Localiza'], data['Valor'], data['Renueva'], data['Otro']
            if data['Localiza'].startswith('Invited by'):
                data['Invitado'], data['Localiza'] = data['Localiza'], 'N/A'

            if data['Activo'].startswith('Active '):
                data['Activo'] = data['Activo'].split()[1]
            if data['Unido'].startswith('Joined '):
                data['Unido'] = data['Unido'][7:]
            if data['Valor'].startswith('$'):
                data['Valor'] = data['Valor'].replace('$', '').replace('/month', '').strip()


        except Exception as e:
            self.logger.warning(f"⚠️ No se pudo procesar un miembro: {e}")
        return data

    def _safe_extract(self, by, selector, default):
        try: return self.driver.find_element(by, selector).text.strip()
        except: return default

    def _safe_extract_from_element(self, parent, by, selector, default):
        try: return parent.find_element(by, selector).text.strip()
        except: return default
    
    def _extract_courses_info(self, profile_url, profile_tab_handle):
        original_window = self.driver.current_window_handle
        gmail_user, contribution_member = 'NA_Email', 'NA_Contrib'
        member_data = {'courses': []}  # Inicializar con lista vacía
        
        try:
            self.driver.switch_to.window(profile_tab_handle)
            self.driver.get(profile_url)

            try:
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except TimeoutException:
                self.logger.warning(f"⚠️ Timeout cargando perfil: {profile_url}")
                return gmail_user, contribution_member, member_data
                
            #contribution_member = self._safe_extract(By.CSS_SELECTOR, '[class*="styled__TypographyWrapper-sc-70zmwu-0 fFYLQx"]', 'NA_Contrib')
            try:
                contribution_member = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[class*="styled__TypographyWrapper-sc-70zmwu-0 fFYLQx"]'))
                ).text.strip()
            except:
                contribution_member = 'NA_Contrib'
            try:
                buttons = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'button.styled__DropdownButton-sc-13jov82-9')))
                
                if buttons:
                    buttons[-1].click()
                    time.sleep(0.5)

                    try:
                        WebDriverWait(self.driver, 3).until(
                            EC.element_to_be_clickable((By.XPATH, "//div[contains(text(),'Membership settings')]"))
                        ).click()
                        
                        time.sleep(1)
                        gmail_user = self._safe_extract(By.CSS_SELECTOR, '[class*="styled__MembershipInfo-sc-gmyn28-1 etpmnD"] span', 'NA_Email')
                        
                        #--------Extracción de cursos--------
                        try:
                            course_elements = WebDriverWait(self.driver, 5).until(
                                EC.presence_of_all_elements_located(
                                    (By.CSS_SELECTOR, '.styled__DesktopNavItem-sc-1p35nnr-3.fQvukM')
                                )
                            )
                            
                            if course_elements:
                                course_elements[0].click()
                                time.sleep(2)
                                
                                titles = WebDriverWait(self.driver, 5).until(
                                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[class*="styled__CourseTitle"]'))
                                )

                                progresses = WebDriverWait(self.driver, 5).until(
                                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '[class*="styled__CourseProgress"]'))
                                )

                                progress_total = 0
                                member_data['courses'] = []  # Reiniciar la lista

                                for i, (title_el, progress_el) in enumerate(zip(titles, progresses)):
                                    title = title_el.text.strip()
                                    progress_str = progress_el.text.strip()

                                    clean_str = progress_str.replace('(', '').replace(')', '').replace(' progress', '').strip()
                                    progress_value = int(clean_str.strip('%')) if '%' in clean_str else 0
                                    progress_total += progress_value

                                    member_data['courses'].append({
                                        'Nro_Curso': i + 1,
                                        'Curso': title,
                                        'Avance_Curso': progress_str,
                                        'Vr. Progreso': progress_value,
                                        'Total': progress_total,
                                        '% Avance': (progress_total * 100) / 29  # Ajustado para 29 cursos
                                    })
                            else:
                                self.logger.warning("No se encontraron elementos de cursos")
                                
                        except TimeoutException:
                            self.logger.debug("⚠️ Timeout extrayendo información de cursos")
                        except Exception as e:
                            self.logger.debug(f"Error menor extrayendo cursos: {e}")
                        #--------Fin extracción cursos--------
                        
                    except TimeoutException:
                        self.logger.debug("⚠️ Timeout en membership settings")
                        
            except Exception as e:
                self.logger.error(f"Error al extraer email: {e}")
                
        except Exception as e:
            self.logger.error(f"Error extrayendo información del perfil: {e}")
        finally:
            self.driver.switch_to.window(original_window)
        
        return gmail_user, contribution_member, member_data

    def _parse_fecha_unido(self, fecha_str):
        try: return datetime.strptime(fecha_str.replace('Joined', '').strip(), '%b %d, %Y')
        except (ValueError, TypeError): return None
        
    def _calculate_permanencia(self, fecha_unido_str):
        fecha_unido = self._parse_fecha_unido(fecha_unido_str)
        if not fecha_unido: return None, None
        delta = datetime.now() - fecha_unido
        return delta.days, math.floor(delta.days / 30)
    
    # --- NUEVAS FUNCIONES DE GUARDADO INCREMENTAL CON MEJORES EN MANEJO DE CONEXIÓN ---
    
    def save_page_to_csv(self, page_data_dicts, file_path):
        """Añade los datos de una página al archivo CSV."""
        try:
            with open(file_path, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.header_csv, extrasaction='ignore')
                
                # Mapeo de claves internas a las del header del CSV
                key_map = {
                    'pagina': 'Pag', 'np': 'Cons_Pag', 'numero': 'Cons_Mbro', 'nombre_miembro': 'Miembro_SK',
                    'nivel': 'Nivel_SK', 'email_gmail': 'Email_Gmail', 'estado_activo': 'Ult_Ingreso',
                    'fecha_unido': 'Fec_Unido', 'valor_membresia': 'Valor', 'contribucion': 'Contribucion',
                    'renueva': 'Renueva', 'email_skool': 'Usuario_SK', 'frase_personal': 'Frase',
                    'localizacion': 'Localizado', 'invito': 'Invito', 'permanencia_dias': 'Dias',
                    'permanencia_meses': 'Meses', 'total_cursos': 'Total_Cursos', 
                    'progreso_total': 'Progreso_Total', 'porcentaje_promedio': 'Porcentaje_Promedio'
                }
                
                # Agregar mapeo para los cursos
                for i in range(1, 30):
                    key_map[f'curso_{i}_nombre'] = f'Curso_{i}_Nombre'
                    key_map[f'curso_{i}_avance'] = f'Curso_{i}_Avance'
                    key_map[f'curso_{i}_progreso'] = f'Curso_{i}_Progreso'
                
                datos_para_csv = []
                for row_interno in page_data_dicts:
                    row_csv = {csv_key: row_interno.get(internal_key) for internal_key, csv_key in key_map.items()}
                    datos_para_csv.append(row_csv)
                
                writer.writerows(datos_para_csv)
            self.logger.info(f"💾 Se añadieron {len(page_data_dicts)} registros al CSV.")
        except Exception as e:
            self.logger.error(f"❌ Error al guardar página en CSV: {e}", exc_info=True)

    def save_page_to_database(self, page_data_dicts):
        """Guarda los datos de una página en PostgreSQL con reconexión automática."""
        if not page_data_dicts: return

        # Obtener timestamp de esta ejecución
        fecha_ejecucion_actual = datetime.now()
        
        # Definir el orden de las columnas incluyendo los cursos
        column_order = [
            'email_skool', 'pagina', 'np', 'numero', 'nombre_miembro', 'nivel', 'email_gmail',
            'estado_activo', 'fecha_unido', 'valor_membresia', 'contribucion',
            'renueva', 'frase_personal', 'localizacion', 'invito', 'invitado',
            'script_ejecutado', 'archivo_generado',
            'permanencia_dias', 'permanencia_meses',
            'total_cursos', 'progreso_total', 'porcentaje_promedio'
        ]

        # Agregar las columnas para los 29 cursos
        for i in range(1, 30):
            column_order.extend([f'curso_{i}_nombre', f'curso_{i}_avance', f'curso_{i}_progreso'])
        
        # Agregar la fecha de ejecución al final
        column_order.append('fecha_ejecucion')

        datos_para_insertar = []
        for member_dict in page_data_dicts:
            member_dict['script_ejecutado'] = self.script_name
            member_dict['archivo_generado'] = self.full_path
            
            # Asegurarse de que todos los campos de cursos existan (incluso si no hay datos)
            for i in range(1, 30):
                if f'curso_{i}_nombre' not in member_dict:
                    member_dict[f'curso_{i}_nombre'] = None
                if f'curso_{i}_avance' not in member_dict:
                    member_dict[f'curso_{i}_avance'] = None
                if f'curso_{i}_progreso' not in member_dict:
                    member_dict[f'curso_{i}_progreso'] = None
            
            # Agregar la fecha de ejecución actual
            member_dict['fecha_ejecucion'] = fecha_ejecucion_actual
            
            # Crear la tupla en el orden correcto
            row_tuple = tuple(member_dict.get(col) for col in column_order)
            datos_para_insertar.append(row_tuple)

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Crear una nueva conexión para cada intento
                with psycopg2.connect(self.connection_string) as conn:
                    with conn.cursor() as cursor:
                        # Construir la consulta dinámicamente
                        columns = ", ".join(column_order)
                        placeholders = ", ".join(["%s"] * len(column_order))
                        query = f"""
                        INSERT INTO miembros_activos_elite_cursos(
                            {columns}
                        ) VALUES ({placeholders})
                        """
                        cursor.executemany(query, datos_para_insertar)
                        self.logger.info(f"🗃️ Se guardaron {len(datos_para_insertar)} registros en PostgreSQL.")
                        return True  # Éxito, salir del bucle de reintentos
            except psycopg2.OperationalError as e:
                retry_count += 1
                self.logger.warning(f"⚠️ Error de conexión a PostgreSQL (intento {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    self.logger.info("🔄 Esperando 5 segundos antes de reintentar...")
                    time.sleep(5)  # Esperar antes de reintentar
            except Exception as e:
                self.logger.error(f"❌ Error al guardar página en PostgreSQL: {e}", exc_info=True)
                return False  # Error no relacionado con la conexión
        
        self.logger.error(f"❌ No se pudo guardar en PostgreSQL después de {max_retries} intentos.")
        return False

    def _procesar_pagina(self, page_number, profile_tab_handle):
        self.logger.info(f"📄 Página {page_number}: Procesando miembros...")
        time.sleep(1)
        datos_pagina_dicts = []
        # La lógica interna de _procesar_pagina se mantiene igual, ya que es correcta
        wait = WebDriverWait(self.driver, 20)
        
        try:
            miembros = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, self.SELECTORS['member_item'])))
        except TimeoutException:
            self.logger.warning(f"⚠️ Timeout esperando miembros en página {page_number}")
            return datos_pagina_dicts

        for i, miembro_element in enumerate(miembros):
            if self.num_members > 0 and self.global_count >= self.num_members: break
            if i % 10 == 0:
                time.sleep(0.5)
            self.global_count += 1
            info_miembro = self._extraer_info_miembro(miembro_element)
            
            profile_link = f'https://www.skool.com/{info_miembro["EmailSkool"]}?g=mentoriavipantoecom'
            gmail_user, contribution_member, info_perfil = self._extract_courses_info(profile_link, profile_tab_handle)
            permanencia_dias, permanencia_meses = self._calculate_permanencia(info_miembro['Unido'])
            
            registro_dict = {
                'pagina': page_number, 'np': i + 1, 'numero': self.global_count,
                'nombre_miembro': info_miembro.get('Miembro'), 'nivel': info_miembro.get('Nivel'),
                'email_gmail': gmail_user, 'estado_activo': info_miembro.get('Activo'), 
                'fecha_unido': info_miembro.get('Unido'), 'valor_membresia': info_miembro.get('Valor'),
                'contribucion': contribution_member, 'renueva': info_miembro.get('Renueva'),
                'email_skool': info_miembro.get('EmailSkool'), 'frase_personal': info_miembro.get('Frase'),
                'localizacion': info_miembro.get('Localiza'), 'invito': info_miembro.get('Invito'),
                'invitado': info_miembro.get('Invitado'), 'permanencia_dias': permanencia_dias,
                'permanencia_meses': permanencia_meses, 'Otro': info_miembro.get('Otro')
            }

            # Agregar información de cursos al registro
            cursos = info_perfil.get('courses', [])
            registro_dict['total_cursos'] = len(cursos)
            registro_dict['progreso_total'] = sum(curso.get('Vr. Progreso', 0) for curso in cursos)
            registro_dict['porcentaje_promedio'] = registro_dict['progreso_total'] / len(cursos) if cursos else 0
            
            # Agregar cada curso individualmente al registro
            for j, curso in enumerate(cursos):
                registro_dict[f'curso_{j+1}_nombre'] = curso.get('Curso', 'N/A')
                registro_dict[f'curso_{j+1}_avance'] = curso.get('Avance_Curso', '0%')
                registro_dict[f'curso_{j+1}_progreso'] = curso.get('Vr. Progreso', 0)

            datos_pagina_dicts.append(registro_dict)

        self.logger.info(f"✔️  Se procesaron {len(datos_pagina_dicts)} miembros en la página {page_number}.")
        return datos_pagina_dicts

    def scrape_miembros(self):
        """
        BUCLE PRINCIPAL: Procesa y GUARDA los datos PÁGINA POR PÁGINA.
        """
        if not self.login(): 
            return
        time.sleep(2)
        self.driver.get(self.URLS['members'])

        try:
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS['member_item']))
            )
            self.logger.info("Página de miembros cargada correctamente.")
        except TimeoutException:
            self.logger.error("No se pudo cargar la página de miembros después de navegar a ella. Terminando.")
            return

        page_number = 1
        
        if self.num_members > 0: 
            self.logger.info(f"🎯 Objetivo: Extraer {self.num_members} miembros")
        else: 
            self.logger.info("🔍 Procesando todos los miembros disponibles")

        with open(self.full_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=self.header_csv)
            writer.writeheader()
        
        # Se crean las pestañas por primera vez
        original_window = self.driver.current_window_handle
        self.driver.switch_to.new_window('tab')
        profile_tab_handle = self.driver.current_window_handle
        self.driver.switch_to.window(original_window)
        
        while True:
            try:
                # --- LÓGICA DE REINICIO MEJORADA ---
                if self.current_session_pages >= self.max_pages_per_session:
                    self.logger.info(f"🔄 Límite de {self.max_pages_per_session} páginas por sesión alcanzado. Reiniciando navegador...")
                    if not self._reiniciar_navegador() or not self.login():
                        self.logger.error("❌ Fallo crítico en el reinicio o re-login. Terminando.")
                        break
                    
                    # CORRECCIÓN: Volver a crear la pestaña de perfiles, ya que la anterior se cerró.
                    self.logger.info("...recreando pestaña para perfiles...")
                    original_window = self.driver.current_window_handle
                    self.driver.switch_to.new_window('tab')
                    profile_tab_handle = self.driver.current_window_handle
                    self.driver.switch_to.window(original_window)

                    # CORRECCIÓN: Navegación directa a la página correcta usando la URL.
                    self.logger.info(f"Reanudando desde la página {page_number}...")
                    # La primera página no usa el parámetro 'p', las siguientes sí.
                    if page_number > 1:
                        page_url = f"{self.URLS['members']}?p={page_number}"
                        self.driver.get(page_url)
                    else:
                        self.driver.get(self.URLS['members'])

                    # Esperar a que la página cargue después de la navegación directa
                    WebDriverWait(self.driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, self.SELECTORS['member_item']))
                    )
                    
                    self.current_session_pages = 0
                # --- FIN DE LA LÓGICA DE REINICIO ---

                if self.num_members > 0 and self.global_count >= self.num_members:
                    self.logger.info(f"✅ Objetivo alcanzado ({self.num_members} miembros)")
                    break

                datos_pagina = self._procesar_pagina(page_number, profile_tab_handle)
                if not datos_pagina:
                    self.logger.info("🏁 Página sin datos. Fin de la paginación.")
                    break
                
                self.save_page_to_csv(datos_pagina, self.full_path)
                
                db_success = self.save_page_to_database(datos_pagina)
                if not db_success:
                    self.logger.warning("⚠️ No se pudieron guardar los datos en la base de datos, pero el proceso continuará.")

                if self.num_members > 0:
                    porcentaje = min(100, int((self.global_count / self.num_members) * 100))
                    self.logger.info(f"📊 Progreso: {self.global_count}/{self.num_members} miembros ({porcentaje}%)")

                try:
                    next_btn = self.driver.find_element(By.XPATH, self.XPATHS['next_button'])
                    self.driver.execute_script("arguments[0].click();", next_btn)
                    WebDriverWait(self.driver, 10).until(EC.staleness_of(next_btn))
                    time.sleep(1)
                    
                    page_number += 1
                    self.current_session_pages += 1
                    
                except (NoSuchElementException, TimeoutException):
                    self.logger.info("🏁 No se encontró el botón 'Next'. Fin de la paginación.")
                    break

            except InvalidSessionIdException:
                self.logger.error("💥 CRASH DEL NAVEGADOR DETECTADO (InvalidSessionIdException). Intentando recuperar...")
                if self._reiniciar_navegador() and self.login():
                    # Al crashear, es mejor empezar de la página actual de nuevo
                    self.logger.info(f"Recuperado. Reintentando la página {page_number}.")
                    # Forzamos un reinicio completo de la sesión y navegación
                    self.current_session_pages = self.max_pages_per_session 
                    continue # El siguiente ciclo activará la lógica de reinicio
                else:
                    self.logger.info("Se han guardado los datos hasta la última página completada. Terminando proceso.")
                    break
            except Exception as e:
                self.logger.error(f"❌ Error inesperado en el bucle principal (página {page_number}): {e}", exc_info=True)
                break
        
        try:
            self.driver.switch_to.window(profile_tab_handle)
            self.driver.close()
        except: 
            pass
        finally:
            try:
                self.driver.switch_to.window(original_window)
            except: 
                pass

    def run(self):
        """
        Orquesta el proceso de scraping. Ahora más simple.
        """
        self.logger.info("🚀 Iniciando el scraper de Skool...")
        self.full_path = os.path.abspath(f"skool_members_{time.strftime('%Y%m%d_%H%M%S')}.csv")

        try:
            if not self._iniciar_driver():
                return
            
            self.scrape_miembros()
            
            self.logger.info("--- Proceso de Scraping Finalizado ---")
            
            # La subida a Dropbox se hace al final con el archivo completo
            self.subir_a_dropbox(self.full_path)
            # if os.path.exists(self.full_path):
            #     os.remove(self.full_path)
            #     self.logger.info(f"🗑️ Archivo local '{self.full_path}' eliminado.")

        except Exception as e:
            self.logger.error(f"❌ Error fatal en la ejecución: {e}", exc_info=True)
        finally:
            # Cerrar navegador y limpiar directorio temporal
            if self.driver:
                try:
                    self.driver.quit()
                    self.logger.info("🛑 Navegador cerrado.")
                except:
                    pass
            
            # Limpiar directorio temporal de Chrome
            if hasattr(self, 'user_data_dir') and os.path.exists(self.user_data_dir):
                try:
                    import shutil
                    shutil.rmtree(self.user_data_dir, ignore_errors=True)
                    self.logger.info("🧹 Directorio temporal de Chrome limpiado.")
                except Exception as e:
                    self.logger.warning(f"⚠️ No se pudo limpiar el directorio temporal: {e}")

if __name__ == "__main__":
    load_dotenv()
    scraper = SkoolScraper()
    scraper.run()