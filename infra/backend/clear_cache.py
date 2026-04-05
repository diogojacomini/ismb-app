#!/usr/bin/env python3
"""
Script auxiliar para limpar o cache da API manualmente após executar pipelines.
"""

import requests
import sys


def clear_cache(base_url: str = "http://localhost:8000"):
    """
    Limpa o cache da API via endpoint POST /api/cache/clear.
    """

    try:
        print(f"Limpando cache em {base_url}...")
        response = requests.post(f"{base_url}/api/cache/clear", timeout=5)
        response.raise_for_status()

        result = response.json()
        print(f"{result['message']}")

        # Show cache status after clearing
        status_response = requests.get(f"{base_url}/api/cache/status", timeout=5)
        status_response.raise_for_status()
        status = status_response.json()

        print("\nStatus do cache:")
        print(f"  - Chaves ativas: {status['live_keys']}")
        print(f"  - TTL configurado: {status['ttl_seconds']}s")

        return 0

    except requests.exceptions.ConnectionError:
        print(f"Erro: Não foi possível conectar à API em {base_url}")
        print("   Certifique-se de que o backend está rodando.")
        return 1

    except requests.exceptions.HTTPError as e:
        print(f"Erro HTTP: {e}")
        return 1

    except Exception as e:
        print(f"Erro inesperado: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(clear_cache())
