import requests
import pandas as pd
import io
import time
from datetime import datetime
import locale
from tqdm import tqdm
import concurrent.futures
import os
import ast

def obtener_appid(juego):
    """
    Busca el AppID de un juego usando la API pública de Steam.
    """
    url = "https://store.steampowered.com/api/storesearch/"
    params = {
        "term": juego,
        "l": "spanish",
        "cc": "CL"
    }

    r = requests.get(url, params=params)
    data = r.json()

    if data.get("items"):
        primer_resultado = data["items"][0]
        return primer_resultado["id"]
    else:
        return None
#----------------------------------------------------------------------------------------------------------------------------------------

def obtener_appids(lista_juegos):
    resultados = {}
    for juego in lista_juegos:
        appid = obtener_appid(juego)
        resultados[juego] = appid
    return resultados
#----------------------------------------------------------------------------------------------------------------------------------------
def fetch_review(appid):
    url = f'https://store.steampowered.com/appreviews/{appid}?json=1'
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        if data.get('success') == 1:
            return {
                'appid': appid,
                'review_score': data['query_summary'].get('review_score'),
                'review_score_desc': data['query_summary'].get('review_score_desc'),
                'total_reviews': data['query_summary'].get('total_reviews')
            }
    except Exception as e:
        print(f"Error con appid {appid}: {e}")
    return {'appid': appid, 'review_score': None, 'review_score_desc': None, 'total_reviews': None}
#----------------------------------------------------------------------------------------------------------------------------------------
def fetch_appdetails(appid):
    url = f'https://store.steampowered.com/api/appdetails?appids={appid}'
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        app_data = data.get(str(appid), {})
        
        if app_data.get('success'):
            info = app_data.get('data', {})

            # Categorías
            categorias = None
            if 'categories' in info:
                categorias = [c.get('description') for c in info['categories']]

            # Fecha de lanzamiento
            fecha_lanzamiento = info.get('release_date', {}).get('date')

            # Géneros
            generos = None
            if 'genres' in info:
                generos = [g.get('description') for g in info['genres']]

            return {
                'appid': appid,
                'name': info.get('name'),
                'type': info.get('type'),
                'required_age': info.get('required_age'),
                'is_free': info.get('is_free'),
                'developers': info.get('developers', [None])[0],
                'price': info.get('price_overview', {}).get('final_formatted') if info.get('price_overview') else 'Free',
                'categories': categorias,
                'release_date': fecha_lanzamiento,
                'genres': generos
            }
    except Exception as e:
        print(f"Error con appid {appid}: {e}")
    time.sleep(1)
    return {'appid': appid, 'name': None, 'type': None, 'required_age': None, 'is_free': None, 'developers': None, 'price': None, 'categories': None, 'release_date': None, 'genres': None}
#----------------------------------------------------------------------------------------------------------------------------------------
def procesar_reviews(df_merged, output_file, batch_size=100, workers=10):
    if os.path.exists(output_file):
        df_existente = pd.read_csv(output_file)
        procesados = set(df_existente['appid'])
        print(f"Reanudando desde archivo parcial con {len(procesados)} registros ya guardados.")
    else:
        df_existente = pd.DataFrame(columns=['appid', 'review_score', 'review_score_desc', 'total_reviews'])
        procesados = set()
    faltantes = df_merged[~df_merged['appid'].isin(procesados)].copy()
    print(f"Faltan por procesar: {len(faltantes)}")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for i, result in enumerate(
            tqdm(executor.map(fetch_review, faltantes['appid']), total=len(faltantes))
        ):
            results.append(result)

            # Guardar por lotes
            if (i + 1) % batch_size == 0 or (i + 1) == len(faltantes):
                df_temp = pd.DataFrame(results)
                df_existente = pd.concat([df_existente, df_temp], ignore_index=True)
                df_existente.to_csv(output_file, index=False)
                results = []
                print(f"Guardado parcial: {len(df_existente)} filas guardadas hasta ahora.")
    print("✔ Proceso completado.")
    return df_existente
#----------------------------------------------------------------------------------------------------------------------------------------
def obtener_appids_multithread(juegos, output_file, batch_size=50, workers=10, func_obtener_appid=None):
    if func_obtener_appid is None:
        raise ValueError("Debes pasar la función func_obtener_appid")
    if os.path.exists(output_file):
        df_existente = pd.read_csv(output_file)
        procesados = set(df_existente['nombre_juego'])
        print(f"Reanudando desde archivo parcial con {len(procesados)} juegos ya guardados.")
    else:
        df_existente = pd.DataFrame(columns=['nombre_juego', 'appid'])
        procesados = set()
    faltantes = [j for j in juegos if j not in procesados]
    print(f"Juegos restantes por procesar: {len(faltantes)}")
    results = []
    def map_func(juego): #funcion de mapeo, por si existen errores
        try:
            appid = func_obtener_appid(juego)
            return {'nombre_juego': juego, 'appid': appid}
        except Exception as e:
            print(f"Error con {juego}: {e}")
            return {'nombre_juego': juego, 'appid': None}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for i, res in enumerate(tqdm(executor.map(map_func, faltantes), total=len(faltantes))):
            results.append(res)
            # Guardado parcial
            if (i + 1) % batch_size == 0 or (i + 1) == len(faltantes):
                df_temp = pd.DataFrame(results)
                df_existente = pd.concat([df_existente, df_temp], ignore_index=True)
                df_existente.to_csv(output_file, index=False)
                results = []
                print(f"Guardado parcial: {len(df_existente)} filas guardadas hasta ahora.")

    print("✔ Todos los AppIDs obtenidos y guardados.")
    return df_existente

#----------------------------------------------------------------------------------------------------------------------------------------
def descargar_detalles_steam(df_merged, output_file, batch_size=100, max_workers=2):
    if os.path.exists(output_file):
        df_existente = pd.read_csv(output_file)
        procesados = set(df_existente['appid'])
        print(f"Reanudando desde archivo parcial con {len(procesados)} registros ya guardados.")
    else:
        df_existente = pd.DataFrame(columns=[
            'appid', 'name', 'type', 'required_age', 'is_free', 
            'detailed_description', 'price', 'developers','categories', 
            'release_date', 'genres'
        ])
        procesados = set()
    faltantes = df_merged[~df_merged['appid'].isin(procesados)].copy()
    print(f"Faltan por procesar: {len(faltantes)}")
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i, result in enumerate(tqdm(executor.map(fetch_appdetails, faltantes['appid']), total=len(faltantes))):
            results.append(result)
            if (i + 1) % batch_size == 0 or (i + 1) == len(faltantes):
                df_temp = pd.DataFrame(results)
                df_existente = pd.concat([df_existente, df_temp], ignore_index=True)
                df_existente.to_csv(output_file, index=False)
                results = []
                print(f"Guardado parcial: {len(df_existente)} filas guardadas hasta ahora.")
    print("✅ Proceso completado.")
    return df_existente
#----------------------------------------------------------------------------------------------------------------------------------------
def parse_fecha(x):
    x = str(x).strip()
    try:
        return pd.to_datetime(x, format='%d %b, %Y')
    except:
        pass
    try:
        return pd.to_datetime(x, format='%b %d, %Y')
    except:
        pass
    try:
        return pd.to_datetime(x, errors='coerce')
    except:
        return pd.NaT
#---------------------------------------------------------------------------------------------------------------------------------------------
def encode_list_column(df, column_name):
    df[column_name] = df[column_name].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )
    categoria_a_num = {}
    contador = 0
    for lista in df[column_name]:
        for item in lista:
            if item not in categoria_a_num:
                categoria_a_num[item] = contador
                contador += 1
    df[column_name + '_numerico'] = df[column_name].apply(
        lambda lista: [categoria_a_num[x] for x in lista]
    )
    
    return df, categoria_a_num
#---------------------------------------------------------------------------------------------------------------------------------------------
def fetch_app_full(appid):
    result = {'appid': appid}

    # ----------------- Fetch reviews -----------------
    try:
        url_reviews = f'https://store.steampowered.com/appreviews/{appid}?json=1'
        response = requests.get(url_reviews, timeout=5)
        data = response.json()
        if data.get('success') == 1:
            result.update({
                'review_score': data['query_summary'].get('review_score'),
                'review_score_desc': data['query_summary'].get('review_score_desc'),
                'total_reviews': data['query_summary'].get('total_reviews')
            })
        else:
            result.update({
                'review_score': None,
                'review_score_desc': None,
                'total_reviews': None
            })
    except Exception as e:
        print(f"Error fetching reviews for appid {appid}: {e}")
        result.update({
            'review_score': None,
            'review_score_desc': None,
            'total_reviews': None
        })

    # ----------------- Fetch app details -----------------
    try:
        url_details = f'https://store.steampowered.com/api/appdetails?appids={appid}'
        response = requests.get(url_details, timeout=5)
        data = response.json()
        app_data = data.get(str(appid), {})

        if app_data.get('success'):
            info = app_data.get('data', {})

            # Categorías
            categorias = [c.get('description') for c in info.get('categories', [])] if 'categories' in info else None
            # Géneros
            generos = [g.get('description') for g in info.get('genres', [])] if 'genres' in info else None
            # Fecha de lanzamiento
            fecha_lanzamiento = info.get('release_date', {}).get('date')

            result.update({
                'name': info.get('name'),
                'type': info.get('type'),
                'required_age': info.get('required_age'),
                'is_free': info.get('is_free'),
                'developers': info.get('developers', [None])[0],
                'price': info.get('price_overview', {}).get('final_formatted') if info.get('price_overview') else 'Free',
                'categories': categorias,
                'release_date': fecha_lanzamiento,
                'genres': generos
            })
        else:
            result.update({
                'name': None, 'type': None, 'required_age': None, 'is_free': None,
                'developers': None, 'price': None, 'categories': None,
                'release_date': None, 'genres': None
            })
    except Exception as e:
        print(f"Error fetching details for appid {appid}: {e}")
        result.update({
            'name': None, 'type': None, 'required_age': None, 'is_free': None,
            'developers': None, 'price': None, 'categories': None,
            'release_date': None, 'genres': None
        })

    time.sleep(1)  # Para no saturar la API
    return result
