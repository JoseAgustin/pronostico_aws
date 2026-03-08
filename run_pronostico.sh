#!/bin/bash -l
# -----------------------------------------------------------------------------
# TÍTULO:       run_pronostico.sh
#
# PROPÓSITO:    Script maestro para orquestar el flujo de trabajo completo del
#               pronóstico WRF-Chem. Incluye manejo de errores robusto,
#               registro de rendimiento y carga de módulos con spack.
#
# VERSIÓN:      2.2
# FECHA:        16/07/2025
# AUTOR:        J. A. Garcia Reynoso <agustin@atmosfera.unam.mx>
# REVISIÓN:     Gemini
#
# USO:          ./run_pronostico.sh
# -----------------------------------------------------------------------------

# --- Opciones de Shell ---
# Salir inmediatamente si un comando falla.
set -e
# Tratar variables no definidas como un error.
set -u

# =============================================================================
# SECCIÓN DE CONFIGURACIÓN PRINCIPAL
# =============================================================================
echo "--- Cargando configuración ---"

# Directorio base para todo el proceso de pronóstico (actualizado)
readonly WORK_DIR="/shared/pronostico"

# Directorios específicos para cada componente del flujo de trabajo
readonly EMIS_DIR="${WORK_DIR}/emis_2016"
readonly DATA_DIR="${WORK_DIR}/data"
readonly WPS_DIR="${WORK_DIR}/wpsprd"
readonly WRF_DIR="${WORK_DIR}/wrfprd"
readonly LOG_DIR="${WORK_DIR}/registro_eventos"
readonly OUTPUT_DIR="${WORK_DIR}/salidas"

# Creación de directorios necesarios si no existen
DIRS=("$WORK_DIR" "$EMIS_DIR" "$DATA_DIR" "$WPS_DIR" "$WRF_DIR" "$LOG_DIR" "$OUTPUT_DIR")

# Verificar y crear
for dir in "${DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo "✔ Directorio existe: $dir"
    else
        echo "⚠ Directorio no existe, creando: $dir"
        mkdir -p "$dir"
        if [ $? -eq 0 ]; then
            echo "   → Creado correctamente."
        else
            echo "   ✘ Error al crear $dir" >&2
            exit 1
        fi
    fi
done
# --- Configuración de Fechas y Archivo de Log ---
readonly START_DATE_FMT=$(date +%Y-%m-%d_00:00:00)
readonly END_DATE_FMT=$(date -d "3 days" +%Y-%m-%d_00:00:00)
readonly GFS_DATE_YMD=$(date +%Y%m%d)
readonly LOG_FILE="${LOG_DIR}/performance_$(date +%Y-%m-%d).log"

# =============================================================================
# DEFINICIÓN DE FUNCIONES
# =============================================================================

# --- Función de Registro ---
log_event() {
    local message="$1"
    local level="$2" # INFO, OK, ERROR, WARNING, METRIC
    echo "$(date +'%Y-%m-%d %H:%M:%S') [${level}] - ${message}" | tee -a "$LOG_FILE"
}

# --- Funcion de Procesamiento de métricas del log de proceso
extract_metric() {
    grep "$1" "$LOG_FILE" | awk '{print $NF}'
}

# --- Función para Ejecutar y Verificar Comandos ---
run_and_check() {
    local command_to_run=("$@")
    log_event "Ejecutando: ${command_to_run[*]}" "INFO"
    
    if ! "${command_to_run[@]}" >> "$LOG_FILE" 2>&1; then
        local exit_code=$?
        log_event "¡ERROR! El comando '${command_to_run[*]}' falló con el código de salida $exit_code." "ERROR"
        log_event "Revisar el archivo de log para más detalles: ${LOG_FILE}" "ERROR"
        exit $exit_code
    fi
}
# --- Función para Formatear Segundos a H:MM:SS ---
# Convierte una duración en segundos a un formato legible.
format_seconds() {
    local total_seconds=$1
    if [[ $total_seconds -lt 0 ]]; then
        total_seconds=0
    fi
    local hours=$((total_seconds / 3600))
    local minutes=$(( (total_seconds % 3600) / 60 ))
    local seconds=$((total_seconds % 60))
    printf "%d:%02d:%02d" "$hours" "$minutes" "$seconds"
}
# --- Función para Descargar Datos GFS ---
run_gfs_download() {
    log_event "Iniciando descarga de datos GFS." "INFO"
    cd "$DATA_DIR"

    for HOUR in $(seq -f "%03g" 0 3 72); do
        local s3_path="s3://noaa-gfs-bdp-pds/gfs.${GFS_DATE_YMD}/00/atmos/gfs.t00z.pgrb2.0p25.f${HOUR}"
        run_and_check aws s3 cp --no-progress --only-show-errors --no-sign-request "$s3_path" .
    done
    log_event "Descarga de datos GFS completada." "OK"
}

# --- Función para Ejecutar WPS ---
run_wps() {
    log_event "Iniciando el pre-procesamiento con WPS." "INFO"
    cd "$WPS_DIR"

    # Namelist actualizado según proceso.sh
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

    run_and_check ./link_grib.csh "${DATA_DIR}/gfs.t"*
    rm -f FILE:*
    run_and_check  ungrib.exe 
    log_event "ungrib.exe completado." "OK"
    rm -f met_em.d*
    run_and_check  metgrid.exe
    log_event "metgrid.exe completado. WPS finalizado." "OK"
}

# --- Función para Ejecutar REAL ---
run_real() {
    log_event "Iniciando real.exe." "INFO"
    cd "$WRF_DIR"
    log_event "Generando namelist.input para REAL..." "INFO"
    # Se genera el namelist.input completo que usará tanto real.exe como wrf.exe
    cat > namelist.input <<- EOF
	&time_control
	 run_days                            = 0,
	 run_hours                           = 72,
	 start_year                          = $(date +%Y),
	 start_month                         = $(date +%m),
	 start_day                           = $(date +%d),
     start_hour                          = 00,   00,
	 end_year                            = $(date -d "3 days" +%Y),
	 end_month                           = $(date -d "3 days" +%m),
	 end_day                             = $(date -d "3 days" +%d),
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
     max_dz                              = 1000.     ! maximum level thickness allowed (m)
     auto_levels_opt                     = 2         ! old(1)  ! new default(2) (also set dzstretch_s, dzstretch_u, dbot, max_dz)
     dzbot                               = 20.       ! thickness of lowest layer (m) for auto_levels_opt=2
     dzstretch_u                         = 1.2       ! upper stretch factor for auto_levels_opt=2
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
	 radt                                = 10,
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
	 specified                           = .true.,
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

    ln -sf "${WPS_DIR}/met_em.d"* .
    rm -f rsl.* wrfinput_d01 wrfbdy_d01
    run_and_check mpiexec -n 4 real.exe
    log_event "real.exe completado." "OK"
}

# --- Función para Ejecutar WRF ---
run_wrf() {
    log_event "Iniciando la ejecución del modelo WRF." "INFO"
    cd "$WRF_DIR"
    
        # Lógica para usar la salida del día anterior como condiciones iniciales
    local yesterday_output="${OUTPUT_DIR}/wrfout_d01_$(date -d "-1 day" +%Y-%m-%d)_00:00:00"
    if [ -f "$yesterday_output" ]; then
        log_event "Usando salida de ayer como condiciones iniciales: ${yesterday_output}" "INFO"
        # Aquí iría la lógica para procesar el archivo (ej. ncks) y enlazarlo
         /shared/mamba/bin/ncks -O -d Time,24,24 -v o3,no2,PM2_5_DRY ${yesterday_output} temp.nc
         /shared/mamba/bin/ncks -A temp.nc wrfinput_d01
         rm temp.nc
    else
        log_event "No se encontró salida de ayer. Ejecutando con condiciones iniciales estándar." "WARNING"
    fi
    
    rm -f rsl.*
    run_and_check \time -v mpiexec -n 8 wrf.exe &> salida 2>> "$LOG_FILE"
       WALL_TIME=$(extract_metric "Elapsed (wall clock) time")
       USER_TIME=$(extract_metric "User time (seconds)")
       SYS_TIME=$(extract_metric "System time (seconds)")
       CPU_PERCENT=$(extract_metric "Percent of CPU this job got")
       MAX_MEM_KB=$(extract_metric "Maximum resident set size")
       MAX_MEM_MB=$(echo "$MAX_MEM_KB / 1024" | bc)
       PAGE_FAULTS=$(extract_metric "Major .page faults")
       FS_OUTPUTS=$(extract_metric "File system outputs")

# Registrar métricas clave
    log_event "Tiempo total ejecución: $WALL_TIME (wall clock)" "METRIC"
    log_event "Tiempo usuario: $USER_TIME segundos" "METRIC"
    log_event "Tiempo sistema: $SYS_TIME segundos" "METRIC"
    log_event "Uso de CPU: $CPU_PERCENT" "METRIC"
    log_event "Memoria máxima utilizada: $MAX_MEM_MB MB (${MAX_MEM_KB}kB)" "METRIC"
    log_event "Page faults: $PAGE_FAULTS" "METRIC"
    log_event "Operaciones I/O: $FS_OUTPUTS" "METRIC" 
    mv wrfout_d01_* "$OUTPUT_DIR/"
        
    local old_output="${OUTPUT_DIR}/wrfout_d01_$(date -d "-7 days" +%Y-%m-%d)_00:00:00"
    if [ -f "$old_output" ]; then
        log_event "Eliminando salida de hace 7 días: ${old_output}" "INFO"
        rm "$old_output"
    fi
}

# =============================================================================
# SCRIPT PRINCIPAL DE EJECUCIÓN
# =============================================================================

log_event "====== INICIO DEL PIPELINE DE PRONÓSTICO WRF ($(date +%Y-%m-%d)) ======" "INFO"
PIPELINE_START_TIME=$SECONDS

# --- PASO 1: Cálculo de emisiones (en segundo plano) ---
log_event "Iniciando el cálculo de emisiones en segundo plano." "INFO"
cd "$EMIS_DIR"
# Llamada al script de emisiones correcto (ecacor.sh)
./ecacor.sh &> "${LOG_DIR}/emisiones.log" &
EMISS_PID=$!

# --- PASO 2: Carga de módulos y descarga de datos GFS ---
log_event "Cargando módulos para WPS y descarga GFS" "INFO"
spack unload
spack load wps
spack load /nz7gqyi # Carga netcdf-fortran
export LD_LIBRARY_PATH=$(spack location -i /nz7gqyi)/lib:$LD_LIBRARY_PATH
log_event "LD_LIBRARY_PATH actualizado." "INFO"

STAGE_START_TIME=$SECONDS
 run_gfs_download
elapsed_seconds=$((SECONDS - STAGE_START_TIME))
log_event "Etapa [Descarga GFS] completada en $(format_seconds $elapsed_seconds) (H:MM:SS)." "METRIC"

# --- PASO 3: Ejecución de WPS ---
STAGE_START_TIME=$SECONDS
run_wps
elapsed_seconds=$((SECONDS - STAGE_START_TIME))
log_event "Etapa [WPS] completada en $(format_seconds $elapsed_seconds) (H:MM:SS)." "METRIC"
# --- PASO 4: Carga de módulos y ejecución de REAL ---
log_event "Cargando módulos para REAL y WRF" "INFO"
spack load /hons4ds
spack load /ozcc2iy
spack load /bjrwihg

STAGE_START_TIME=$SECONDS
run_real
elapsed_seconds=$((SECONDS - STAGE_START_TIME))
log_event "Etapa [REAL] completada en $(format_seconds $elapsed_seconds) (H:MM:SS)." "METRIC"

# --- PASO 5: Esperar emisiones y ejecutar WRF ---
log_event "Esperando a que el cálculo de emisiones (PID: ${EMISS_PID}) finalice..." "INFO"
wait $EMISS_PID
log_event "Cálculo de emisiones finalizado." "OK"

# Enlazar las emisiones al directorio de WRF
#ln -sf "${EMIS_DIR}/inventario/centro/wrfchemi_d01_"* "${WRF_DIR}/"

STAGE_START_TIME=$SECONDS
run_wrf
elapsed_seconds=$((SECONDS - STAGE_START_TIME))
log_event "Etapa [WRF] completada en $(format_seconds $elapsed_seconds) (H:MM:SS)." "METRIC"

log_event "====== PROCESO DE PRONÓSTICO WRF-chem FINALIZADO ======" "OK"
total_elapsed_seconds=$((SECONDS - PIPELINE_START_TIME))
log_event "Tiempo total del pronostico: $(format_seconds $total_elapsed_seconds) (H:MM:SS)." "METRIC"

# --- PASO 6: genera archivo de registro de eventos
#
STAGE_START_TIME=$SECONDS
cd $WORK_DIR
bash analiza2.sh
elapsed_seconds=$((SECONDS - STAGE_START_TIME))
log_event "====== PROCESO DE SUPERVISION FINALIZADO ======" "OK""

exit 0
