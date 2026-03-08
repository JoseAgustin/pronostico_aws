#!/bin/bash

DEFAULT_LOG_DIR="/shared/pronostico/registro_eventos"
LOG_DIR="${1:-$DEFAULT_LOG_DIR}"

# Archivo JSON de salida
OUTPUT_JSON="${LOG_DIR}/datos.json"

if [ ! -d "$LOG_DIR" ]; then
    echo "Error: El directorio de logs no fue encontrado en '${LOG_DIR}'."
    exit 1
fi

if ! ls "${LOG_DIR}/performance_"*.log 1> /dev/null 2>&1; then
    echo "No se encontraron archivos performance_*.log en '${LOG_DIR}'."
    exit 0
fi

# ---------------- FUNCIONES ----------------

get_time() {
    local pattern="$1"
    local file="$2"
    local time_line
    time_line=$(grep "$pattern" "$file")

    if [ -n "$time_line" ]; then
        echo "$time_line" | awk '{print $(NF-1)}'
    else
        echo "N/A"
    fi
}

get_wrf_metric() {
    local pattern="$1"
    local file="$2"
    local field_pos="$3"

    local metric_line
    metric_line=$(grep "$pattern" "$file")

    if [ -n "$metric_line" ]; then
        echo "$metric_line" | awk -v pos_str="$field_pos" '{
            if (pos_str == "NF") print $NF;
            else if (pos_str == "NF-2") print $(NF-2);
            else print "0";
        }'
    else
        echo "0"
    fi
}

get_total_emis_time() {
    local pattern="$1"
    local file="$2"
    local total_seconds=0
    local time_values
    time_values=$(grep "$pattern" "$file" || true)

    if [ -n "$time_values" ]; then
        while read -r line; do
            time_str=$(echo "$line" | awk '{print $(NF-1)}')
            h=$(echo "$time_str" | cut -d: -f1)
            m=$(echo "$time_str" | cut -d: -f2)
            s=$(echo "$time_str" | cut -d: -f3)
            ((total_seconds += h * 3600 + m * 60 + s))
        done <<< "$time_values"
    fi

    if [ "$total_seconds" -eq 0 ]; then
        echo "N/A"
    else
        printf "%d:%02d:%02d" \
        $((total_seconds / 3600)) \
        $(((total_seconds % 3600) / 60)) \
        $((total_seconds % 60))
    fi
}

# ---------------- CABECERA TABLA ----------------

printf "%-12s | %-8s | %-12s | %-4s | %-10s | %-10s | %-10s | %-10s | %-10s | %-8s | %-8s | %-10s | %-12s\n" \
"FECHA" "ERRORES" "ADVERTENCIAS" "OK" "T.Emis" "T.GFS" "T.WPS" "T.REAL" "T.WRF" "CPU%" "Mem(MB)" "I/O" "T.TOTAL"

echo "-------------------------------------------------------------------------------------------------------------------------------------------------------------------"

# Inicia archivo JSON
echo "[" > "$OUTPUT_JSON"

first_record=true

for log_file in "${LOG_DIR}/performance_"*.log; do

    date_str=$(basename "$log_file" .log | sed 's/performance_//')
    emisiones_log_file="${LOG_DIR}/emisiones_${date_str}.log"

    errors=$(cat "$log_file" "$emisiones_log_file" 2>/dev/null | grep -c '\[ERROR\]' || true)
    warnings=$(cat "$log_file" "$emisiones_log_file" 2>/dev/null | grep -c '\[WARNING\]' || true)
    oks=$(cat "$log_file" "$emisiones_log_file" 2>/dev/null | grep -c '\[OK\]' || true)

    time_gfs=$(get_time "Etapa \[Descarga GFS\]" "$log_file")
    time_wps=$(get_time "Etapa \[WPS\]" "$log_file")
    time_real=$(get_time "Etapa \[REAL\]" "$log_file")
    time_wrf=$(get_time "Etapa \[WRF\]" "$log_file")
    time_total=$(get_time "Tiempo total del pronostico" "$log_file")

    time_emis="N/A"
    if [ -f "$emisiones_log_file" ]; then
        time_emis=$(get_total_emis_time "Tiempo total para el DÍA" "$emisiones_log_file")
    fi

    cpu_usage=$(get_wrf_metric "Uso de CPU:" "$log_file" "NF")
    cpu_usage=$(echo "$cpu_usage" | tr -d '%')

    max_mem=$(get_wrf_metric "Memoria máxima utilizada:" "$log_file" "NF-2")
    io_ops=$(get_wrf_metric "Operaciones I/O:" "$log_file" "NF")

    # Salida en tabla
    printf "%-12s | %-8s | %-12s | %-4s | %-10s | %-10s | %-10s | %-10s | %-10s | %-8s | %-8s | %-10s | %-12s\n" \
    "$date_str" "$errors" "$warnings" "$oks" "$time_emis" "$time_gfs" "$time_wps" "$time_real" "$time_wrf" \
    "${cpu_usage}%" "${max_mem}" "${io_ops}" "$time_total"

    # Construcción JSON
    if [ "$first_record" = true ]; then
        first_record=false
    else
        echo "," >> "$OUTPUT_JSON"
    fi

    cat >> "$OUTPUT_JSON" <<EOF
{
  "fecha": "$date_str",
  "errores": $errors,
  "advertencias": $warnings,
  "ok": $oks,
  "tiempo_emisiones": "$time_emis",
  "tiempo_gfs": "$time_gfs",
  "tiempo_wps": "$time_wps",
  "tiempo_real": "$time_real",
  "tiempo_wrf": "$time_wrf",
  "cpu": $cpu_usage,
  "memoria_mb": $max_mem,
  "io_operaciones": $io_ops,
  "tiempo_total": "$time_total"
}
EOF

done

echo "]" >> "$OUTPUT_JSON"

echo "-------------------------------------------------------------------------------------------------------------------------------------------------------------------"
echo "Análisis completado."
echo "Archivo JSON generado en: $OUTPUT_JSON"

exit 0
