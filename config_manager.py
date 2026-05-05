import json
import os
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

STORAGE_PATH = os.getenv("STORAGE_PATH", "./files/")
if STORAGE_PATH.endswith("/") or STORAGE_PATH.endswith("\\"):
            STORAGE_PATH = STORAGE_PATH[:-1]
CONFIG_FILE = STORAGE_PATH + "/territories_time_ranges.json"


def load_time_ranges() -> List[Dict[str, str]]:
    """
    Carica i range temporali per i territori dal file JSON.
    Ritorna una lista di dizionari con territory_id, start_time e end_time.
    """
    if not os.path.exists(CONFIG_FILE):
        logger.warning(f"File di configurazione '{CONFIG_FILE}' non trovato. Ritorno lista vuota.")
        return []
    
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Caricati {len(data)} territory time ranges dal file '{CONFIG_FILE}'")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Errore nel parsing del file JSON '{CONFIG_FILE}': {e}")
        return []
    except Exception as e:
        logger.error(f"Errore nella lettura del file '{CONFIG_FILE}': {e}")
        return []


def save_time_ranges(time_ranges: List[Dict[str, str]]) -> bool:
    """
    Salva i range temporali per i territori nel file JSON.
    
    Args:
        time_ranges: Lista di dizionari con 'territory_id', 'start_time' e 'end_time'
    
    Returns:
        True se il salvataggio è riuscito, False altrimenti
    """
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(time_ranges, f, indent=2, ensure_ascii=False)
        logger.info(f"Salvati {len(time_ranges)} territory time ranges nel file '{CONFIG_FILE}'")
        return True
    except Exception as e:
        logger.error(f"Errore nel salvataggio del file '{CONFIG_FILE}': {e}")
        return False


def get_time_range_for_territory(territory_id: str) -> Optional[Dict[str, str]]:
    """
    Ottiene il range temporale (start_time, end_time) per un specifico territorio.
    
    Args:
        territory_id: ID del territorio
    
    Returns:
        Dizionario con 'start_time' e 'end_time', oppure None se non trovato
    """
    time_ranges = load_time_ranges()
    for tr in time_ranges:
        if tr.get('territory_id') == territory_id:
            return {
                'start_time': tr.get('start_time'),
                'end_time': tr.get('end_time')
            }
    logger.warning(f"Nessun range temporale trovato per il territorio '{territory_id}'")
    return None


def add_or_update_time_range(territory_id: str, start_time: str, end_time: str) -> bool:
    """
    Aggiunge o aggiorna il range temporale per un territorio.
    
    Args:
        territory_id: ID del territorio
        start_time: Data/ora di inizio (formato ISO 8601)
        end_time: Data/ora di fine (formato ISO 8601)
    
    Returns:
        True se l'operazione è riuscita, False altrimenti
    """
    time_ranges = load_time_ranges()
    
    # Cerca se il territorio esiste già
    found = False
    for tr in time_ranges:
        if tr.get('territory_id') == territory_id:
            tr['start_time'] = start_time
            tr['end_time'] = end_time
            found = True
            logger.info(f"Aggiornato range temporale per il territorio '{territory_id}'")
            break
    
    # Se non trovato, aggiungi nuovo
    if not found:
        time_ranges.append({
            'territory_id': territory_id,
            'start_time': start_time,
            'end_time': end_time
        })
        logger.info(f"Aggiunto nuovo range temporale per il territorio '{territory_id}'")
    
    return save_time_ranges(time_ranges)


def remove_time_range(territory_id: str) -> bool:
    """
    Rimuove il range temporale per un territorio.
    
    Args:
        territory_id: ID del territorio
    
    Returns:
        True se l'operazione è riuscita, False altrimenti
    """
    time_ranges = load_time_ranges()
    original_length = len(time_ranges)
    time_ranges = [tr for tr in time_ranges if tr.get('territory_id') != territory_id]
    
    if len(time_ranges) < original_length:
        logger.info(f"Rimosso range temporale per il territorio '{territory_id}'")
        return save_time_ranges(time_ranges)
    else:
        logger.warning(f"Nessun range temporale trovato per il territorio '{territory_id}' da rimuovere")
        return False
