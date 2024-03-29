# ETL

## Configuración del Entorno Virtual

Para garantizar un entorno de desarrollo aislado y reproducible, se recomienda utilizar un entorno virtual. Siga los pasos a continuación para configurar el entorno virtual:

```bash
# Crear un entorno virtual (puede usar cualquier nombre, aquí se usa 'venv' como ejemplo)
python -m venv venv

# Activar el entorno virtual (comandos varían según el sistema operativo)
# En Windows
venv\Scripts\activate

# En Linux/Mac
source venv/bin/activate
```

## Instalación de Dependencias

Instale las dependencias del proyecto utilizando el siguiente comando:

```bash
pip install -r requirements.txt
```

## Configuración del archivo .env

Asegúrese de crear un archivo .env en la raíz del proyecto y complete las variables necesarias. Puede seguir el ejemplo proporcionado en el archivo .env.template incluido en el repositorio.

## Credenciales de Google (creds.json)

Para utilizar las credenciales de Google, siga estos pasos:
- Acceda a Goole Cloud Platform.
- Cree un nuevo proyecto o seleccione uno existente.
- En el panel de navegación, vaya a "Credenciales".
- Haga clic en "Crear credenciales" y seleccione "Cuenta de servicio".
- Complete el formulario y descargue el archivo JSON que contiene las credenciales.
- Guarde el archivo JSON descargado como creds.json en la raíz del proyecto.

# Ejecución del script
Una vez que el entorno virtual está activado y las dependencias están instaladas, puede ejecutar el proyecto con el siguiente comando:

```bash
python main.py --date 2023-10-01
```