# Sistema de Pronóstico Numérico WRF-Chem

**run_pronostico.sh** — Script maestro de orquestación del pipeline de pronóstico atmosférico WRF-Chem para la Zona Metropolitana del Valle de México.

---

## Descripción del Proyecto

Este repositorio contiene la automatización operativa del sistema de pronóstico numérico de calidad del aire basado en **WRF-Chem**. El script descarga datos de condiciones iniciales y de frontera del modelo global GFS (NOAA), ejecuta el pre-procesamiento WPS, la inicialización con `real.exe` y la simulación principal con `wrf.exe`, todo en un entorno de cómputo de 8 cores sin gestor de colas (sin SLURM).

El sistema está diseñado para **ejecución automática vía cron**, con énfasis en robustez operativa: detección de fallos, reintentos automáticos de descarga y registro detallado de eventos.

---

## Flujo del Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                  PIPELINE WRF-Chem (ciclo 00Z)                  │
└─────────────────────────────────────────────────────────────────┘

  [PASO 1] Emisiones (background) ──────────────────────────┐
                                                            │
  [PASO 2] Descarga GFS (S3/NOAA) ◄─── sistema reintentos   │
                │                                           │
                ▼                                           │
  [PASO 3] Pre-procesamiento WPS                            │
           ├── link_grib.csh                                │
           ├── ungrib.exe                                   │
           └── metgrid.exe                                  │
                │                                           │
                ▼                                           │
  [PASO 4] Inicialización WRF                               │
           └── real.exe (4 MPI)                             │
                │                                           │
                ▼                             ◄─────────────┘
  [PASO 5] Sincroniza emisiones → wrf.exe (8 MPI)
                │
                ▼
  [PASO 6] Análisis de supervisión (analiza2.sh)
```

---

## Requisitos del Sistema

| Componente    | Versión mínima | Notas                                 |
|---------------|----------------|---------------------------------------|
| Bash          | 4.4+           | `set -euo pipefail` requiere bash ≥4  |
| OpenMPI / MPICH | Cualquiera   | Compatible con el WRF compilado       |
| GNU Coreutils | cualquiera     | `date`, `seq`, `bc`, `awk`            |
| Linux         | kernel ≥3.x    | Probado en Amazon Linux 2 / CentOS 7  |


### Hardware mínimo recomendado

- **CPUs**: 8 cores (configuración operativa actual)
- **RAM**: 32 GB (WRF-Chem con química requiere ≥16 GB)
- **Almacenamiento**: 100 GB libres en `WORK_DIR`
- **Red**: acceso a internet para S3 (NOAA GFS público)

---

## Dependencias de Software

### Científicas (gestionadas con Spack)

| Paquete          | Hash Spack | Función                        |
|------------------|-----------|-------------------------------|
| WPS 4.4          | `kawqtu…` | Pre-procesamiento WRF          |
| WRF-Chem         | `/hons4ds` | Modelo de pronóstico           |
| HDF5             | `/ozcc2iy` | I/O NetCDF                     |
| NetCDF-C         | `/bjrwihg` | Formato de salida              |
| netcdf-fortran   | `/nz7gqyi` | Interfaces Fortran NetCDF      |
| NCO (ncks)       | mamba     | Manipulación de archivos WRF   |

### Herramientas del sistema

- **AWS CLI** v2 — descarga de datos GFS desde `s3://noaa-gfs-bdp-pds`
- **Spack** — gestión de entornos de software científico
- **bc**, **awk** — cálculo de métricas en tiempo de ejecución

---

## Estructura de Directorios

```
/shared/pronostico/
├── run_pronostico.sh       # Script maestro (este repositorio)
├── analiza2.sh             # Script de análisis post-proceso
├── emis_2016/
│   └── ecacor.sh           # Cálculo de inventario de emisiones
├── data/                   # Datos GFS descargados (gfs.t00z.pgrb2.0p25.f*)
├── wpsprd/                 # Directorio de trabajo de WPS
│   ├── link_grib.csh
│   ├── ungrib.exe
│   ├── metgrid.exe
│   └── met_em.d01.*        # Archivos generados por metgrid
├── wrfprd/                 # Directorio de trabajo de WRF
│   ├── real.exe
│   ├── wrf.exe
│   ├── wrfinput_d01        # Generado por real.exe
│   └── wrfbdy_d01          # Generado por real.exe
├── salidas/                # Archivos wrfout_d01_* (salidas finales)
└── registro_eventos/
    ├── performance_YYYY-MM-DD.log   # Log principal del pipeline
    └── emisiones_YYYY-MM-DD.log     # Log del proceso de emisiones
```

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-org/wrf-chem-pronostico.git /shared/pronostico
cd /shared/pronostico
```

### 2. Dar permisos de ejecución

```bash
chmod +x run_pronostico.sh analiza2.sh emis_2016/ecacor.sh
```

### 3. Verificar AWS CLI

```bash
aws s3 ls s3://noaa-gfs-bdp-pds/gfs.$(date +%Y%m%d)/00/atmos/ \
    --no-sign-request | head -5
```

### 4. Verificar entorno Spack

```bash
spack load wps && which ungrib.exe
spack load /hons4ds && which wrf.exe
```

### 5. Configurar cron (ejecución diaria a las 06:00 UTC)

```bash
crontab -e
# Agregar la línea:
0 6 * * * /shared/pronostico/run_pronostico.sh \
    >> /shared/pronostico/registro_eventos/cron.log 2>&1
```

---

## Ejemplo de Ejecución

### Manual

```bash
cd /shared/pronostico
./run_pronostico.sh
```

### Con seguimiento en tiempo real

```bash
./run_pronostico.sh &
tail -f registro_eventos/performance_$(date +%Y-%m-%d).log
```

---

## Sistema de Reintentos GFS

Cuando los datos GFS aún no están disponibles en los servidores de NOAA al momento de la consulta, el script implementa la siguiente lógica:

```
Intento 1
   │── Éxito → continúa el pipeline
   └── Fallo → espera 5 minutos
        │
        ▼
Intento 2
   │── Éxito → continúa el pipeline
   └── Fallo → espera 5 minutos
        │
        ▼
Intento 3 (final)
   │── Éxito → continúa el pipeline
   └── Fallo → ABORTA con registro de error
```

### Parámetros configurables

| Variable          | Valor default | Descripción                             |
|-------------------|:-------------:|-----------------------------------------|
| `GFS_MAX_RETRIES` | `3`           | Número máximo de intentos totales       |
| `GFS_RETRY_WAIT`  | `300`         | Segundos de espera entre reintentos     |

Para modificar estos valores, editar la **Sección 1** del script:

```bash
readonly GFS_MAX_RETRIES=5    # 5 intentos
readonly GFS_RETRY_WAIT=600   # 10 minutos entre intentos
```

---

## Ejemplo de Salida del Log

```
2025-07-16 06:00:01 [INFO] - ╔══════════════════════════════════════════════╗
2025-07-16 06:00:01 [INFO] - ║   INICIO DEL PIPELINE DE PRONÓSTICO WRF-Chem ║
2025-07-16 06:00:01 [INFO] - ║   Fecha: 2025-07-16  Ciclo: 00Z              ║
2025-07-16 06:00:01 [INFO] - ╚══════════════════════════════════════════════╝
2025-07-16 06:00:02 [INFO] - PASO 1: Lanzando cálculo de emisiones en segundo plano...
2025-07-16 06:00:02 [INFO] - Proceso de emisiones lanzado (PID: 14821).
2025-07-16 06:00:03 [INFO] - PASO 2: Cargando módulos WPS...
2025-07-16 06:00:05 [INFO] -   Intento 1/3 para: gfs.t00z.pgrb2.0p25.f000
2025-07-16 06:00:05 [WARNING] -   ✘ Intento 1/3 fallido: gfs.t00z.pgrb2.0p25.f000
2025-07-16 06:00:05 [WARNING] -   ⏳ Esperando 5 minuto(s) (300s) antes del reintento...
2025-07-16 06:05:06 [INFO] -   Intento 2/3 para: gfs.t00z.pgrb2.0p25.f000
2025-07-16 06:05:48 [OK]   -   ✔ Archivo obtenido exitosamente (intento 2): gfs.t00z.pgrb2.0p25.f000
...
2025-07-16 06:45:12 [OK]   - Descarga GFS completada: 25 archivos obtenidos.
2025-07-16 06:45:12 [METRIC] - Etapa [Descarga GFS] completada en 0:45:07.
...
2025-07-16 14:32:15 [METRIC] - Tiempo de ejecución (wall clock): 7:46:53
2025-07-16 14:32:15 [METRIC] - Memoria máxima: 18432 MB (18874368 kB)
2025-07-16 14:32:15 [OK]   - ╔══════════════════════════════════════════╗
2025-07-16 14:32:15 [OK]   - ║   PIPELINE WRF-Chem FINALIZADO           ║
2025-07-16 14:32:15 [OK]   - ║   Tiempo total: 8:32:14 (H:MM:SS)        ║
2025-07-16 14:32:15 [OK]   - ╚══════════════════════════════════════════╝
```

---

## Licencia

Instituto de Ciencias de la Atmósfera y Cambio Climático, UNAM.  
Contacto: agustin@atmosfera.unam.mx
