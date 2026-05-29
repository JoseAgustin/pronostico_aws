# Release Notes — run_pronostico.sh

## v3.0.2 — 2026-03-09

### Corrección de errores

#### `date`: extra operand al generar el resumen final del pipeline
- **Síntoma:** Al completar el pipeline exitosamente se producía el error
  `date: extra operand '%H:%M:%S'` en la línea del bloque de resumen final.
- **Causa:** El formato de `date` contenía un espacio sin comillas dentro de
  una sustitución de comandos `$(...)`. El shell interpretaba `%H:%M:%S` como
  un segundo argumento posicional independiente al binario `date`.
- **Corrección:** Se agregaron comillas simples alrededor del formato completo:
  ```bash
  # Antes (incorrecto):
  $(date +%Y-%m-%d %H:%M:%S)

  # Después (correcto):
  $(date +'%Y-%m-%d %H:%M:%S')
  ```
- **Archivos afectados:** `run_pronostico.sh` — bloque de resumen final (Sección 8).

---

## v3.0.1 — 2026-03-09

### Corrección de compatibilidad con Bash 4.2

Este release corrige cuatro incompatibilidades con Bash 4.2 bajo `set -euo pipefail`.
Todas derivan del mismo patrón: en Bash 4.2 con `set -e`, una expresión
aritmética `(( expr ))` que evalúa a **cero** retorna exit code 1 y aborta
el script silenciosamente.

#### `(( attempt++ ))` — bucle de reintentos GFS
- **Síntoma:** El pipeline abortaba sin mensaje en la primera iteración del
  bucle de reintentos si la descarga fallaba en el intento 1.
- **Causa:** `(( attempt++ ))` con `attempt=1` produce resultado pre-incremento
  `0`, lo que Bash 4.2 interpreta como fallo bajo `set -e`.
- **Corrección:**
  ```bash
  # Antes:
  (( attempt++ ))

  # Después:
  attempt=$(( attempt + 1 ))
  ```

#### `(( total_files++ ))` — contador de archivos en `run_gfs_download`
- **Síntoma:** El pipeline abortaba en la primera iteración del bucle de
  descarga, antes de intentar bajar cualquier archivo GFS.
- **Causa:** Idéntica al caso anterior; en la primera iteración
  `total_files=0`, por lo que `(( total_files++ ))` retorna exit code 1.
- **Corrección:**
  ```bash
  # Antes:
  (( total_files++ ))

  # Después:
  total_files=$(( total_files + 1 ))
  ```

#### `local var=$(( expr ))` — función `format_seconds`
- **Síntoma:** El pipeline abortaba al registrar la duración de cualquier
  etapa cuyo tiempo resultara en `hours`, `minutes` o `seconds` igual a `0`.
- **Causa:** En Bash 4.2, `local` propaga el exit code de la sustitución de
  comandos. Si `$(( expr ))` vale `0`, la asignación `local var=0` retorna
  exit code 1 bajo `set -e`.
- **Corrección:** Separar la declaración `local` de la asignación aritmética:
  ```bash
  # Antes:
  local hours=$(( total_seconds / 3600 ))
  local minutes=$(( (total_seconds % 3600) / 60 ))
  local seconds=$(( total_seconds % 60 ))

  # Después:
  local hours minutes seconds
  hours=$(( total_seconds / 3600 ))
  minutes=$(( (total_seconds % 3600) / 60 ))
  seconds=$(( total_seconds % 60 ))
  ```

#### `local wait_min=$(( expr ))` — función `_download_gfs_hour`
- **Síntoma y causa:** Idénticos al caso anterior. Si `GFS_RETRY_WAIT` es
  múltiplo de 60, `wait_min` resultaba `0` y el pipeline abortaba durante
  la espera entre reintentos.
- **Corrección:** Separar declaración de asignación (mismo patrón que arriba).

### Nota general sobre Bash 4.2

> En Bash 4.2 con `set -e` activo, se deben evitar dos patrones:
>
> 1. **`(( var++ ))` o `(( var-- ))`** cuando el resultado puede ser `0`.
>    Usar siempre `var=$(( var + 1 ))`.
> 2. **`local var=$(( expr ))`** cuando el resultado puede ser `0`.
>    Declarar `local var` en una línea y asignar en la siguiente.
>
> Ambos patrones funcionan correctamente en Bash 5.x, por lo que no
> generaban error en entornos de desarrollo modernos.

---

## v3.0.0 — 2026-03-08

### Nuevas funcionalidades

#### Sistema de reintentos automático para descarga de datos GFS
- Implementado en dos funciones dedicadas: `_download_single_gfs_file()` y
  `_download_gfs_hour()`.
- Hasta `GFS_MAX_RETRIES=3` intentos totales por archivo.
- Espera configurable de `GFS_RETRY_WAIT=300` segundos (5 minutos) entre intentos.
- Validación de integridad del archivo descargado (existencia y tamaño > 0).
- En caso de fallo definitivo: registro estructurado del error y abort
  controlado del pipeline con `exit 1`.

#### Variables de configuración centralizadas (Sección 1)
Se incorporaron como `readonly` los parámetros operativos clave:

| Variable           | Default | Descripción                          |
|--------------------|---------|--------------------------------------|
| `GFS_MAX_RETRIES`  | `3`     | Máximo de intentos de descarga       |
| `GFS_RETRY_WAIT`   | `300`   | Segundos entre reintentos            |
| `WRF_NPROCS`       | `8`     | Procesadores MPI para `wrf.exe`      |
| `REAL_NPROCS`      | `4`     | Procesadores MPI para `real.exe`     |
| `FORECAST_HOURS`   | `72`    | Horizonte de pronóstico en horas     |
| `GFS_INTERVAL_HOURS` | `3`   | Intervalo temporal entre archivos GFS|

#### Modo estricto de shell completo
- Añadido `pipefail` a las opciones de shell: `set -euo pipefail`.
- La versión anterior solo usaba `set -e` y `set -u`.

#### Mejoras en el sistema de logging
- Registro de cada intento de descarga con número de intento y nombre de archivo.
- Registro del tiempo de espera entre reintentos.
- Bloque de error visualmente delimitado en caso de fallo definitivo de GFS.
- Registro del resultado del proceso de emisiones en background (éxito o advertencia).
- Banner de inicio y resumen final con tiempo total del pipeline.

### Cambios de comportamiento

- El proceso de emisiones (`ecacor.sh`) ya no aborta el pipeline si termina
  con error: registra `[WARNING]` y continúa. Ajustar si las emisiones son
  obligatorias en la instalación.
- Las fechas de fin en `namelist.input` ahora se calculan a partir de
  `FORECAST_HOURS` en lugar del valor fijo `date -d "3 days"`.
- El log de emisiones incluye la fecha en el nombre: `emisiones_YYYY-MM-DD.log`.

### Correcciones respecto a v2.2

- Eliminado el error de comilla doble extra en la línea final (`"OK""` → `"OK"`).
- Variables locales de métricas en `run_wrf()` declaradas correctamente con `local`.
- `LD_LIBRARY_PATH` exportado con patrón seguro `${LD_LIBRARY_PATH:-}` para
  evitar error de variable no definida con `set -u`.

---

## v2.2 — 2025-07-16

Versión base de referencia. Pipeline funcional con `set -e` y `set -u`,
sin sistema de reintentos para descarga GFS.
