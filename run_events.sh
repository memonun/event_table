#!/bin/bash

LOGFILE="run_events.log"
ERRORLOG="run_events_error.log"

echo "Çalıştırma başladı: $(date)" >> "$LOGFILE"

SCRIPTS=(
    "Cron/biletinial_artist_promoter_desc.py"
    "Cron/bubilet.py"
    "Cron/bugece.py"
    "Cron/passo_promoter_artist.py"
)

for script in "${SCRIPTS[@]}"; do
    echo "---- $script çalıştırılıyor: $(date) ----" >> "$LOGFILE"
    if python3 "$script" >> "$LOGFILE" 2> >(tee -a "$ERRORLOG" >> "$LOGFILE" >&2); then
        echo "$script başarıyla tamamlandı." >> "$LOGFILE"
    else
        echo "⚠️ $script çalıştırılırken hata oluştu!" >> "$LOGFILE"
    fi
done

echo "Git işlemleri başlıyor: $(date)" >> "$LOGFILE"
git add .
git commit -m "Cron dosyaları otomatik çalıştırıldı: $(date)" >> "$LOGFILE" 2>&1
git push >> "$LOGFILE" 2>&1

echo "Tüm işlemler tamamlandı: $(date)" >> "$LOGFILE"