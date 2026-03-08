#!/bin/bash -l
# =============================================================================
# TÍTULO:       run_pronostico.sh
#
# PROPÓSITO:    Script maestro para orquestar el flujo de trabajo completo del
#               pronóstico WRF-Chem. Incluye manejo de errores robusto,
#               registro de rendimiento, sistema de reintentos para descarga
#               de datos GFS y carga de módulos con spack.
#
# VERSIÓN:      3.0
# FECHA:        $(date +%Y-%m-%d)
# AUTORES:      J. A. Garcia Reynoso <agustin@atmosfera.unam.mx>
# REVISIÓN:     Ingeniería de automatización HPC
#
# USO:          ./run_pronostico.sh
#
# EJECUCIÓN AUTOMÁTICA (cron):
#   0 6 * * * /shared/pronostico/run_pronostico.sh >> /shared/pronostico/registro_eventos/cron.log 2>&1
#
# VARIABLES CONFIGURABLES CLAVE:
#   GFS_MAX_RETRIES   - Número máximo de intentos de descarga (default: 3)
#   GFS_RETRY_WAIT    - Segundos de espera entre reintentos (default: 300 = 5 min)
#   WRF_NPROCS        - Número de procesadores para WRF (default: 8)
#   REAL_NPROCS       - Número de procesadores para real.exe (default: 4)
#
# FLUJO DEL PIPELINE:
#   [Emisiones en BG] → [Descarga GFS (con reintentos)] → [WPS] → [REAL] →
#   [Espera emisiones] → [WRF] → [Análisis de supervisión]
# =============================================================================

# --- Opciones de Shell: modo estricto de producción ---
# -e : abortar si cualquier comando falla
# -u : abortar si se usa una variable no definida
# -o pipefail : un pipe falla si algún componente falla
set -euo pipefail

# =============================================================================
# SECCIÓN 1: CONFIGURACIÓN PRINCIPAL
# Todas las rutas y parámetros del sistema se definen aquí como variables
# readonly para evitar modificaciones accidentales durante la ejecución.
# =============================================================================

# --- Rutas del sistema ---
readonly WORK_DIR="/shared/pronostico"
readonly EMIS_DIR="${WORK_DIR}/emis_2016"
readonly DATA_DIR="${WORK_DIR}/data"
readonly WPS_DIR="${WORK_DIR}/wpsprd"
readonly WRF_DIR="${WORK_DIR}/wrfprd"
readonly LOG_DIR="${WORK_DIR}/registro_eventos"
readonly OUTPUT_DIR="${WORK_DIR}/salidas"

# --- Herramientas externas ---
readonly NCKS_BIN="/shared/mamba/bin/ncks"

# --- Parámetros de ejecución paralela ---
# Ajustar según el hardware disponible (máquina de 8 cores)
readonly WRF_NPROCS=8
readonly REAL_NPROCS=4

# --- Parámetros del sistema de reintentos GFS ---
# GFS_MAX_RETRIES: total de intentos (1 inicial + 2 reintentos = 3 total)
# GFS_RETRY_WAIT: tiempo de espera en segundos entre intentos (300 = 5 minutos)
readonly GFS_MAX_RETRIES=3
readonly GFS_RETRY_WAIT=300

# --- Parámetros de pronóstico WRF ---
# Horizonte de pronóstico en horas y resolución temporal de los datos GFS
readonly FORECAST_HOURS=72
readonly GFS_INTERVAL_HOURS=3

# --- Configuración de fechas ---
# Todas las fechas se derivan del momento de ejecución del script.
# Para un pronóstico con corrida del ciclo 00Z del día en curso:
readonly START_DATE_FMT=$(date +%Y-%m-%d_00:00:00)
readonly END_DATE_FMT=$(date -d "${FORECAST_HOURS} hours" +%Y-%m-%d_%H:00:00)
readonly GFS_DATE_YMD=$(date +%Y%m%d)
readonly LOG_FILE="${LOG_DIR}/performance_$(date +%Y-%m-%d).log"

# =============================================================================
# SECCIÓN 2: INICIALIZACIÓN DEL ENTORNO
# Verificación y creación de directorios requeridos antes de cualquier
# operación de I/O o cómputo.
# =============================================================================
_init_directories() {
    local dirs=("$WORK_DIR" "$EMIS_DIR" "$DATA_DIR" "$WPS_DIR" "$WRF_DIR" "$LOG_DIR" "$OUTPUT_DIR")
    for dir in "${dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            echo "✔ Directorio existe: $dir"
        else
            echo "⚠ Creando directorio: $dir"
            mkdir -p "$dir" || { echo "✘ Error al crear $dir" >&2; exit 1; }
        fi
    done
}

# Inicializar directorios antes de que el LOG_FILE esté disponible
_init_directories

# =============================================================================
# SECCIÓN 3: FUNCIONES UTILITARIAS
# Funciones de uso general reutilizables en todo el pipeline.
# =============================================================================

# -----------------------------------------------------------------------------
# log_event: Registra un mensaje con marca de tiempo y nivel de severidad.
#
# Parámetros:
#   $1 - Mensaje a registrar
#   $2 - Nivel: INFO | OK | ERROR | WARNING | METRIC
#
# Salida:
#   Escribe en stdout y agrega al LOG_FILE.
# -----------------------------------------------------------------------------
log_event() {
    local message="$1"
    local level="${2:-INFO}"
    local timestamp
    timestamp=$(date +'%Y-%m-%d %H:%M:%S')
    echo "${timestamp} [${level}] - ${message}" | tee -a "$LOG_FILE"
}

# -----------------------------------------------------------------------------
# format_seconds: Convierte segundos a formato legible H:MM:SS.
#
# Parámetros:
#   $1 - Duración total en segundos (entero no negativo)
#
# Salida:
#   Imprime la cadena formateada en stdout.
# -----------------------------------------------------------------------------
format_seconds() {
    local total_seconds="${1:-0}"
    [[ $total_seconds -lt 0 ]] && total_seconds=0
    # NOTA Bash 4.2: separar 'local' de la asignación aritmética.
    # 'local var=$(( expr ))' con set -e aborta el script si el resultado es 0,
    # porque 'local' propaga el exit code de la sustitución de comandos.
    local hours minutes seconds
    hours=$(( total_seconds / 3600 ))
    minutes=$(( (total_seconds % 3600) / 60 ))
    seconds=$(( total_seconds % 60 ))
    printf "%d:%02d:%02d" "$hours" "$minutes" "$seconds"
}

# -----------------------------------------------------------------------------
# extract_metric: Extrae el último campo de una línea del log que coincida
#                 con el patrón dado. Usado para capturar métricas de /usr/bin/time.
#
# Parámetros:
#   $1 - Patrón de búsqueda (grep)
#
# Salida:
#   Imprime el valor encontrado o la cadena vacía.
# -----------------------------------------------------------------------------
extract_metric() {
    grep "$1" "$LOG_FILE" 2>/dev/null | awk '{print $NF}' | tail -1
}

# -----------------------------------------------------------------------------
# run_and_check: Ejecuta un comando y verifica su código de salida.
#               Registra el inicio, resultado y cualquier error en el log.
#               Aborta el pipeline si el comando falla.
#
# Parámetros:
#   $@ - Comando completo con sus argumentos
# -----------------------------------------------------------------------------
run_and_check() {
    local cmd_str="$*"
    log_event "Ejecutando: ${cmd_str}" "INFO"
    if ! "$@" >> "$LOG_FILE" 2>&1; then
        local exit_code=$?
        log_event "¡ERROR! Comando fallido: '${cmd_str}' (código: ${exit_code})" "ERROR"
        log_event "Consulte el log para detalles: ${LOG_FILE}" "ERROR"
        exit "${exit_code}"
    fi
}

# =============================================================================
# SECCIÓN 4: DESCARGA DE DATOS GFS CON SISTEMA DE REINTENTOS
# =============================================================================

# -----------------------------------------------------------------------------
# _download_single_gfs_file: Intenta descargar un único archivo GFS desde S3.
#
# Parámetros:
#   $1 - Ruta S3 completa del archivo a descargar
#
# Retorna:
#   0 si la descarga fue exitosa y el archivo existe con tamaño > 0
#   1 si la descarga falló o el archivo resultó vacío/inexistente
# -----------------------------------------------------------------------------
_download_single_gfs_file() {
    local s3_path="$1"
    local filename
    filename=$(basename "$s3_path")

    # Intentar la descarga silenciosa desde S3 público de NOAA
    if aws s3 cp \
        --no-progress \
        --only-show-errors \
        --no-sign-request \
        "$s3_path" . >> "$LOG_FILE" 2>&1; then

        # Verificar que el archivo existe y no está vacío
        if [[ -s "${DATA_DIR}/${filename}" ]]; then
            return 0
        else
            log_event "Archivo descargado pero vacío: ${filename}" "WARNING"
            rm -f "${DATA_DIR}/${filename}"
            return 1
        fi
    else
        return 1
    fi
}

# -----------------------------------------------------------------------------
# _download_gfs_hour: Descarga el archivo GFS para una hora de pronóstico
#                     con el mecanismo de reintentos completo.
#
# Lógica de reintentos:
#   - Hasta GFS_MAX_RETRIES intentos totales
#   - Entre intentos: espera GFS_RETRY_WAIT segundos
#   - Si todos los intentos fallan: registra error y aborta el pipeline
#
# Parámetros:
#   $1 - Hora de pronóstico formateada con ceros (ej: "000", "003", "072")
# -----------------------------------------------------------------------------
_download_gfs_hour() {
    local hour="$1"
    local s3_path="s3://noaa-gfs-bdp-pds/gfs.${GFS_DATE_YMD}/00/atmos/gfs.t00z.pgrb2.0p25.f${hour}"
    local filename
    filename=$(basename "$s3_path")
    local attempt=1

    log_event "  Descargando: ${filename}" "INFO"

    while [[ $attempt -le $GFS_MAX_RETRIES ]]; do
        log_event "  Intento ${attempt}/${GFS_MAX_RETRIES} para: ${filename}" "INFO"

        if _download_single_gfs_file "$s3_path"; then
            log_event "  ✔ Archivo obtenido exitosamente (intento ${attempt}): ${filename}" "OK"
            return 0
        fi

        # La descarga falló en este intento
        log_event "  ✘ Intento ${attempt}/${GFS_MAX_RETRIES} fallido: ${filename}" "WARNING"

        # Si hay más intentos disponibles, esperar antes del siguiente
        if [[ $attempt -lt $GFS_MAX_RETRIES ]]; then
            # NOTA Bash 4.2: separar 'local' de la asignación aritmética
            local wait_min
            wait_min=$(( GFS_RETRY_WAIT / 60 ))
            log_event "  Esperando ${wait_min} minuto(s) (${GFS_RETRY_WAIT}s) antes del reintento..." "WARNING"
            sleep "$GFS_RETRY_WAIT"
        fi

        # NOTA Bash 4.2 + set -e: (( attempt++ )) falla con exit code 1
        # cuando attempt pasa de 0 a 1 (resultado 0 antes del incremento).
        # Usar la forma segura: attempt=$(( attempt + 1 ))
        attempt=$(( attempt + 1 ))
    done

    # Todos los intentos agotados: registrar fallo definitivo y abortar
    log_event "╔══════════════════════════════════════════════════════════╗" "ERROR"
    log_event "║  FALLO DEFINITIVO EN DESCARGA GFS                        ║" "ERROR"
    log_event "║  Archivo: ${filename}" "ERROR"
    log_event "║  Fecha GFS: ${GFS_DATE_YMD} / Ciclo: 00Z                 ║" "ERROR"
    log_event "║  Intentos realizados: ${GFS_MAX_RETRIES}                 ║" "ERROR"
    log_event "║  Los datos GFS NO estaban disponibles en el servidor.     ║" "ERROR"
    log_event "║  Verifique disponibilidad en: s3://noaa-gfs-bdp-pds/     ║" "ERROR"
    log_event "╚══════════════════════════════════════════════════════════╝" "ERROR"
    log_event "PIPELINE ABORTADO por falta de datos GFS." "ERROR"
    exit 1
}

# -----------------------------------------------------------------------------
# run_gfs_download: Función principal de descarga GFS.
#                   Descarga todos los archivos del ciclo 00Z para las horas
#                   0 a FORECAST_HOURS en intervalos de GFS_INTERVAL_HOURS.
#                   Cada archivo utiliza el mecanismo de reintentos individual.
# -----------------------------------------------------------------------------
run_gfs_download() {
    log_event "━━━ ETAPA: Descarga de datos GFS ━━━" "INFO"
    log_event "Parámetros: Fecha=${GFS_DATE_YMD}, Ciclo=00Z, Horas=0-${FORECAST_HOURS}h@${GFS_INTERVAL_HOURS}h" "INFO"
    log_event "Sistema de reintentos: max=${GFS_MAX_RETRIES} intentos, espera=${GFS_RETRY_WAIT}s entre intentos" "INFO"
    cd "$DATA_DIR"

    local total_files=0
    local failed_files=0

    # Iterar sobre todas las horas de pronóstico en el intervalo configurado
    for hour in $(seq -f "%03g" 0 "$GFS_INTERVAL_HOURS" "$FORECAST_HOURS"); do
        # NOTA Bash 4.2 + set -e: (( total_files++ )) aborta cuando total_files==0
        total_files=$(( total_files + 1 ))
        # _download_gfs_hour aborta el pipeline si falla definitivamente
        _download_gfs_hour "$hour"
    done

    log_event "Descarga GFS completada: ${total_files} archivos obtenidos." "OK"
}

# =============================================================================
# SECCIÓN 5: PRE-PROCESAMIENTO WPS
# =============================================================================

# -----------------------------------------------------------------------------
# run_wps: Ejecuta el flujo completo de WPS (geogrid/ungrib/metgrid).
#          Genera el namelist.wps con las fechas del pronóstico actual,
#          enlaza los archivos GRIB, ejecuta ungrib y metgrid.
#
# Pre-requisitos:
#   - Datos GFS descargados en DATA_DIR
#   - Ejecutables WPS disponibles en WPS_DIR (link_grib.csh, ungrib.exe, metgrid.exe)
# -----------------------------------------------------------------------------
run_wps() {
    log_event "━━━ ETAPA: Pre-procesamiento WPS ━━━" "INFO"
    cd "$WPS_DIR"

    # Generar namelist.wps dinámico con fechas del ciclo actual
    log_event "Generando namelist.wps para: ${START_DATE_FMT} → ${END_DATE_FMT}" "INFO"
    cat > namelist.wps <<- END_WPS
	&share
	 wrf_core = 'ARW',
	 max_dom = 1,
	 start_date = '${START_DATE_FMT}',
	 end_date   = '${END_DATE_FMT}',
	 interval_seconds = 10800,
	 io_form_geogrid = 2,
	 opt_output_from_geogrid_path = '${WPS_DIR}',
	/
	&geogrid
	 parent_id         = 1,
	 parent_grid_ratio = 1,
	 i_parent_start    = 1,
	 j_parent_start    = 1,
	 e_we          =  90,
	 e_sn          =  90,
	 geog_data_res = 'default',
	 dx =  3000,
	 dy =  3000,
	 map_proj =  'lambert',
	 ref_lat   = 19.42449,
	 ref_lon   = -98.88641,
	 truelat1  = 19.5,
	 truelat2  = 19.5,
	 stand_lon = -99.1133,
	 geog_data_path = '/shared/spack/opt/spack/linux-amzn2-icelake/intel-2021.7.1/wps-4.4-kawqtuws6z7nxudsyzjen4lqwo7behnl/geo',
	/
	&ungrib
	 out_format = 'WPS',
	 prefix = 'FILE',
	/
	&metgrid
	 fg_name = 'FILE',
	 io_form_metgrid = 2,
	 opt_output_from_metgrid_path = '$WPS_DIR',
	 opt_metgrid_tbl_path = '$WPS_DIR',
	/
END_WPS

    # Enlazar archivos GRIB y ejecutar ungrib
    run_and_check ./link_grib.csh "${DATA_DIR}/gfs.t"*
    rm -f FILE:*
    run_and_check ungrib.exe
    log_event "ungrib.exe completado." "OK"

    # Ejecutar metgrid para interpolación horizontal
    rm -f met_em.d*
    run_and_check metgrid.exe
    log_event "metgrid.exe completado. WPS finalizado." "OK"
}

# =============================================================================
# SECCIÓN 6: INICIALIZACIÓN DE WRF (real.exe)
# =============================================================================

# -----------------------------------------------------------------------------
# run_real: Genera el namelist.input y ejecuta real.exe con MPI.
#           Crea wrfinput_d01 y wrfbdy_d01 necesarios para wrf.exe.
#
# Pre-requisitos:
#   - Archivos met_em.* disponibles en WPS_DIR
#   - Ejecutable real.exe disponible en WRF_DIR
# -----------------------------------------------------------------------------
run_real() {
    log_event "━━━ ETAPA: Inicialización real.exe ━━━" "INFO"
    cd "$WRF_DIR"

    log_event "Generando namelist.input (start=${START_DATE_FMT}, end=${END_DATE_FMT})" "INFO"

    cat > namelist.input <<- EOF
	&time_control
	 run_days                            = 0,
	 run_hours                           = ${FORECAST_HOURS},
	 start_year                          = $(date +%Y),
	 start_month                         = $(date +%m),
	 start_day                           = $(date +%d),
	 start_hour                          = 00,   00,
	 end_year                            = $(date -d "${FORECAST_HOURS} hours" +%Y),
	 end_month                           = $(date -d "${FORECAST_HOURS} hours" +%m),
	 end_day                             = $(date -d "${FORECAST_HOURS} hours" +%d),
	 end_hour                            = 00,   00,
	 interval_seconds                    = 10800,
	 input_from_file                     = .true.,
	 history_interval                    = 60,
	 frames_per_outfile                  = 1000,
	 io_form_history                     = 2,
	 io_form_restart                     = 2,
	 io_form_input                       = 2,
	 io_form_boundary                    = 2,
	 io_form_auxinput4                   = 2,
	 io_form_auxinput5                   = 2,
	 iofields_filename                   ="ignore_vars_d1.txt","ignore_vars_d2.txt"
	 ignore_iofields_warning             = .true.,
	 auxinput4_interval                  = 360, 360, 360,
	 auxinput5_interval_m                = 60, 60, 60
	 auxinput1_inname                    = "met_em.d<domain>.<date>"
	 auxinput4_inname                    = "wrflowinp_d<domain>"
	 auxinput5_inname                    = "wrfchemi.d<domain>.<date>"
	 debug_level                         = 0,
	/
	
	&domains
	 max_dz                              = 1000.
	 auto_levels_opt                     = 2
	 dzbot                               = 20.
	 dzstretch_u                         = 1.2
	 time_step                           = 12,
	 time_step_fract_num                 = 0,
	 time_step_fract_den                 = 1,
	 max_dom                             = 1,
	 e_we                                = 90,
	 e_sn                                = 90,
	 e_vert                              = 40,
	 p_top_requested                     = 5000.0,
	 num_metgrid_levels                  = 34,
	 num_metgrid_soil_levels             = 4,
	 dx                                  = 3000,
	 dy                                  = 3000,
	 grid_id                             = 1,
	 parent_id                           = 0,
	 i_parent_start                      = 1,
	 j_parent_start                      = 1,
	 parent_grid_ratio                   = 1,
	 parent_time_step_ratio              = 1,
	 feedback                            = 0,
	/
	
	&physics
	 physics_suite                       = 'TROPICAL'
	 mp_physics                          = -1,
	 cu_physics                          = 5,
	 ra_lw_physics                       = -1,
	 ra_sw_physics                       = -1,
	 bl_pbl_physics                      = -1,
	 sf_sfclay_physics                   = -1,
	 sf_surface_physics                  = -1,
	 cu_diag                             = 1,
	 radt                                = 10,
	 cugd_avedx                          = 1,
	 cudt                                = 5,
	 mp_zero_out                         = 2,
	 isfflx                              = 1,
	 ifsnow                              = 0,
	 icloud                              = 1,
	 num_land_cat                        = 24,
	 surface_input_source                = 3,
	 num_soil_layers                     = 4,
	 sf_urban_physics                    = 0,
	/
	
	&fdda
	 grid_fdda                           =   1,    1,    1,
	 gfdda_inname                        = "wrffdda_d<domain>",
	 gfdda_interval_m                    = 180,  360,  360,
	 gfdda_end_h                         =9999, 9999, 9999,
	 fgdt                                =   0,    0,   0,
	 if_no_pbl_nudging_uv                =   1,    1,   1,
	 if_no_pbl_nudging_t                 =   1,    1,   1,
	 if_no_pbl_nudging_q                 =   1,    1,   1,
	 if_zfac_uv                          =   1,    1,   1,
	 k_zfac_uv                           =  10,   10,  10,
	 if_zfac_t                           =   0,    0,  10,
	 k_zfac_t                            =  10,   10,  10,
	 if_zfac_q                           =   0,    0,   0,
	 k_zfac_q                            =  10,   10,  10,
	 guv                                 = 0.0003, 0.0003, 0.0003,
	 gt                                  = 0.0003, 0.0003, 0.0003,
	 gq                                  = 0.0003, 0.0003, 0.0003,
	 if_ramping                          = 1,
	 dtramp_min                          = 60.0
	 io_form_gfdda                       =   2,
	/
	
	&dynamics
	 rk_ord                              = 3,
	 hybrid_opt                          = 2,
	 w_damping                           = 0,
	 diff_opt                            = 1,
	 km_opt                              = 4,
	 non_hydrostatic                     = .true.,
	 moist_adv_opt                       = 1,      1,      1,
	 scalar_adv_opt                      = 1,      1,      1,
	 chem_adv_opt                        = 2,      2,       0,
	 tke_adv_opt                         = 2,      2,       0,
	 time_step_sound                     = 4,      4,      4,
	 h_mom_adv_order                     = 5,      5,      5,
	 v_mom_adv_order                     = 3,      3,      3,
	 h_sca_adv_order                     = 5,      5,      5,
	 v_sca_adv_order                     = 3,      3,      3,
	 gwd_opt                             = 0,
	/
	
	&bdy_control
	 spec_bdy_width                      = 10,
	 spec_zone                           = 1,
	 relax_zone                          = 9,
	 specified                           = .true., .false.,.false.,
	 nested                              = .false., .true., .true.,
	/
	
	&grib2
	/
	
	&namelist_quilt
	 nio_tasks_per_group = 0,
	 nio_groups = 1,
	/
	
	&chem
	 kemit                               = 8,
	 chem_opt                            = 108,      105, 30,
	 bioemdt                             = 20,
	 ne_area                             = 210,
	 photdt                              = 20,
	 chemdt                              = 5,
	 io_style_emissions                  = 2,
	 emiss_inpt_opt                      = 1,          1,
	 emiss_opt                           = 3,          9,
	 chem_in_opt                         = 0,          0,
	 phot_opt                            = 1,          1,
	 gas_drydep_opt                      = 1,          1,
	 aer_drydep_opt                      = 1,          1,
	 bio_emiss_opt                       = 1,          1,
	 gas_bc_opt                          = 1,          1,
	 gas_ic_opt                          = 1,          1,
	 aer_bc_opt                          = 1,          1,
	 aer_ic_opt                          = 1,          1,
	 gaschem_onoff                       = 1,          1,
	 aerchem_onoff                       = 1,          1,
	 wetscav_onoff                       = 0,          0,
	 cldchem_onoff                       = 0,          0,
	 vertmix_onoff                       = 1,          1,
	 chem_conv_tr                        = 1,          1,
	 seas_opt                            = 0,
	 dust_opt                            = 3,
	 dmsemis_opt                         = 0,
	 biomass_burn_opt                    = 0,          0,
	 plumerisefire_frq                   = 30,         0,
	 have_bcs_chem                       = .false., .false., .false.,
	 aer_ra_feedback                     = 1, 1,
	 aer_op_opt                          = 1, 1,
	 vprm_opt                            = "VPRM_table_TROPICS",
	 opt_pars_out                        = 0,
	 diagnostic_chem                     = 0,
	/
EOF

    # Enlazar archivos de condiciones de frontera interpoladas
    ln -sf "${WPS_DIR}/met_em.d"* .
    rm -f rsl.* wrfinput_d01 wrfbdy_d01

    run_and_check mpiexec -n "${REAL_NPROCS}" real.exe
    log_event "real.exe completado exitosamente." "OK"
}

# =============================================================================
# SECCIÓN 7: EJECUCIÓN DEL MODELO WRF
# =============================================================================

# -----------------------------------------------------------------------------
# run_wrf: Ejecuta wrf.exe con MPI, captura métricas de rendimiento,
#          maneja condiciones iniciales del día anterior y limpia archivos
#          de salida con más de 7 días de antigüedad.
#
# Pre-requisitos:
#   - wrfinput_d01 y wrfbdy_d01 en WRF_DIR (generados por real.exe)
#   - Archivos de emisiones wrfchemi_d01_* enlazados en WRF_DIR
# -----------------------------------------------------------------------------
run_wrf() {
    log_event "━━━ ETAPA: Ejecución del modelo WRF ━━━" "INFO"
    cd "$WRF_DIR"

    # --- Condiciones iniciales de química del día anterior ---
    # Si existe la salida de ayer, se extraen variables de química (O3, NO2, PM2.5)
    # para usarlas como condiciones iniciales del campo de química (spin-up continuo).
    local yesterday_output="${OUTPUT_DIR}/wrfout_d01_$(date -d '-1 day' +%Y-%m-%d)_00:00:00"
    if [[ -f "$yesterday_output" ]]; then
        log_event "Usando salida de ayer como condiciones iniciales: ${yesterday_output}" "INFO"
        "${NCKS_BIN}" -O -d Time,24,24 -v o3,no2,PM2_5_DRY "${yesterday_output}" temp.nc \
            >> "$LOG_FILE" 2>&1
        "${NCKS_BIN}" -A temp.nc wrfinput_d01 >> "$LOG_FILE" 2>&1
        rm -f temp.nc
        log_event "Condiciones iniciales de química aplicadas desde ayer." "OK"
    else
        log_event "No se encontró salida de ayer (${yesterday_output}). Usando condiciones iniciales estándar." "WARNING"
    fi

    # --- Ejecución principal de WRF con medición de rendimiento ---
    rm -f rsl.*
    log_event "Lanzando wrf.exe con ${WRF_NPROCS} procesadores MPI..." "INFO"

    # /usr/bin/time -v captura métricas detalladas de uso de recursos del sistema
    \time -v mpiexec -n "${WRF_NPROCS}" wrf.exe > salida 2>> "$LOG_FILE" || {
        log_event "¡ERROR CRÍTICO! wrf.exe falló. Revisar rsl.error.* y ${LOG_FILE}" "ERROR"
        exit 1
    }

    # --- Captura y registro de métricas de rendimiento ---
    log_event "── Métricas de rendimiento WRF ──" "METRIC"
    local wall_time user_time sys_time cpu_pct max_mem_kb max_mem_mb page_faults fs_out
    wall_time=$(extract_metric "Elapsed (wall clock) time")
    user_time=$(extract_metric "User time (seconds)")
    sys_time=$(extract_metric "System time (seconds)")
    cpu_pct=$(extract_metric "Percent of CPU this job got")
    max_mem_kb=$(extract_metric "Maximum resident set size")
    max_mem_mb=$(echo "${max_mem_kb:-0} / 1024" | bc)
    page_faults=$(extract_metric "Major .page faults")
    fs_out=$(extract_metric "File system outputs")

    log_event "Tiempo de ejecución (wall clock): ${wall_time}" "METRIC"
    log_event "Tiempo de usuario: ${user_time} s" "METRIC"
    log_event "Tiempo de sistema: ${sys_time} s" "METRIC"
    log_event "Uso de CPU: ${cpu_pct}" "METRIC"
    log_event "Memoria máxima: ${max_mem_mb} MB (${max_mem_kb} kB)" "METRIC"
    log_event "Page faults: ${page_faults}" "METRIC"
    log_event "Operaciones I/O (escritura): ${fs_out}" "METRIC"

    # --- Mover salidas al directorio de resultados ---
    mv wrfout_d01_* "$OUTPUT_DIR/" && \
        log_event "Archivos wrfout_d01_* movidos a ${OUTPUT_DIR}/" "OK"

    # --- Limpieza de salidas antiguas (política de retención: 7 días) ---
    local old_output="${OUTPUT_DIR}/wrfout_d01_$(date -d '-7 days' +%Y-%m-%d)_00:00:00"
    if [[ -f "$old_output" ]]; then
        log_event "Eliminando salida con 7 días de antigüedad: ${old_output}" "INFO"
        rm -f "$old_output"
    fi
}

# =============================================================================
# SECCIÓN 8: PIPELINE PRINCIPAL DE EJECUCIÓN
# Orquesta todas las etapas del flujo de pronóstico en secuencia,
# midiendo el tiempo de cada etapa y el total del proceso.
# =============================================================================

log_event "╔══════════════════════════════════════════════════════════════╗" "INFO"
log_event "║   INICIO DEL PIPELINE DE PRONÓSTICO WRF-Chem                ║" "INFO"
log_event "║   Fecha: $(date +%Y-%m-%d)  Ciclo: 00Z                      ║" "INFO"
log_event "║   Horizon: ${FORECAST_HOURS}h | Reintentos GFS: ${GFS_MAX_RETRIES} | Espera: $((GFS_RETRY_WAIT/60))min  ║" "INFO"
log_event "╚══════════════════════════════════════════════════════════════╝" "INFO"

PIPELINE_START_TIME=$SECONDS

# ---------------------------------------------------------------------------
# PASO 1: Cálculo de emisiones (en segundo plano)
# Se lanza inmediatamente para aprovechar el tiempo de descarga y WPS.
# El PID se guarda para sincronizar antes de ejecutar WRF.
# ---------------------------------------------------------------------------
log_event "PASO 1: Lanzando cálculo de emisiones en segundo plano..." "INFO"
cd "$EMIS_DIR"
./ecacor.sh > "${LOG_DIR}/emisiones_$(date +%Y-%m-%d).log" 2>&1 &
EMISS_PID=$!
log_event "Proceso de emisiones lanzado (PID: ${EMISS_PID})." "INFO"

# ---------------------------------------------------------------------------
# PASO 2: Carga de módulos para WPS y descarga de datos GFS
# ---------------------------------------------------------------------------
log_event "PASO 2: Cargando módulos WPS..." "INFO"
spack unload
spack load wps
spack load /nz7gqyi           # netcdf-fortran para WPS
export LD_LIBRARY_PATH
LD_LIBRARY_PATH="$(spack location -i /nz7gqyi)/lib:${LD_LIBRARY_PATH:-}"
log_event "LD_LIBRARY_PATH configurado." "INFO"

STAGE_START_TIME=$SECONDS
run_gfs_download
elapsed_seconds=$(( SECONDS - STAGE_START_TIME ))
log_event "Etapa [Descarga GFS] completada en $(format_seconds $elapsed_seconds)." "METRIC"

# ---------------------------------------------------------------------------
# PASO 3: Ejecución de WPS
# ---------------------------------------------------------------------------
STAGE_START_TIME=$SECONDS
run_wps
elapsed_seconds=$(( SECONDS - STAGE_START_TIME ))
log_event "Etapa [WPS] completada en $(format_seconds $elapsed_seconds)." "METRIC"

# ---------------------------------------------------------------------------
# PASO 4: Carga de módulos para WRF y ejecución de real.exe
# ---------------------------------------------------------------------------
log_event "PASO 4: Cargando módulos WRF..." "INFO"
spack load /hons4ds            # WRF y dependencias MPI
spack load /ozcc2iy            # HDF5
spack load /bjrwihg            # NetCDF-C

STAGE_START_TIME=$SECONDS
run_real
elapsed_seconds=$(( SECONDS - STAGE_START_TIME ))
log_event "Etapa [REAL] completada en $(format_seconds $elapsed_seconds)." "METRIC"

# ---------------------------------------------------------------------------
# PASO 5: Sincronización con cálculo de emisiones y ejecución de WRF
# ---------------------------------------------------------------------------
log_event "PASO 5: Esperando finalización de emisiones (PID: ${EMISS_PID})..." "INFO"
if wait "$EMISS_PID"; then
    log_event "Cálculo de emisiones finalizado correctamente." "OK"
else
    log_event "¡ADVERTENCIA! El proceso de emisiones terminó con error (PID: ${EMISS_PID}). Revisar ${LOG_DIR}/emisiones_$(date +%Y-%m-%d).log" "WARNING"
    # No se aborta el pipeline; WRF puede ejecutarse sin emisiones actualizadas
    # si las del día anterior están enlazadas. Ajustar según política operativa.
fi

# Enlazar archivos de emisiones al directorio de WRF
# Descomentar la siguiente línea cuando el inventario esté disponible:
# ln -sf "${EMIS_DIR}/inventario/centro/wrfchemi_d01_"* "${WRF_DIR}/"

STAGE_START_TIME=$SECONDS
run_wrf
elapsed_seconds=$(( SECONDS - STAGE_START_TIME ))
log_event "Etapa [WRF] completada en $(format_seconds $elapsed_seconds)." "METRIC"

# ---------------------------------------------------------------------------
# PASO 6: Análisis de supervisión y generación de reporte de eventos
# ---------------------------------------------------------------------------
log_event "PASO 6: Ejecutando análisis de supervisión..." "INFO"
STAGE_START_TIME=$SECONDS
cd "$WORK_DIR"
bash analiza2.sh >> "$LOG_FILE" 2>&1
elapsed_seconds=$(( SECONDS - STAGE_START_TIME ))
log_event "Etapa [Supervisión] completada en $(format_seconds $elapsed_seconds)." "METRIC"

# ---------------------------------------------------------------------------
# RESUMEN FINAL DEL PIPELINE
# ---------------------------------------------------------------------------
total_elapsed=$(( SECONDS - PIPELINE_START_TIME ))
log_event "╔══════════════════════════════════════════════════════════════╗" "OK"
log_event "║   PIPELINE WRF-Chem FINALIZADO EXITOSAMENTE                 ║" "OK"
log_event "║   Fecha: $(date +%Y-%m-%d %H:%M:%S)                          ║" "OK"
log_event "║   Tiempo total: $(format_seconds $total_elapsed) (H:MM:SS)            ║" "OK"
log_event "╚══════════════════════════════════════════════════════════════╝" "OK"

exit 0
