# Planning actual del productor — reprise del 8 de octubre

Rama: `checkpoint/2026-10-08-reprise`. El traspaso vigente es
[REPRISE_2026-10-08](REPRISE_2026-10-08.md). El planning completo, pruebas y
estado del consumidor están en
[lakehouse-copilot](https://github.com/Manuelma4/lakehouse-copilot/blob/checkpoint/2026-10-08-reprise/docs/PLANNING_ACTUAL_2026-10-08.md).

| Tramo | Estado |
| --- | --- |
| Recuperación privada y originales anteriores | Completada; capturas anteriores conservadas |
| Fuentes gratuitas | Fútbol de seis competiciones y VALORANT con cinco páginas por fase; cuotas respetadas |
| Contexto de temporada/local/visitante | Publicado desde el original observado, con hashes y relojes verificables |
| Histórico v2 | Siete paquetes: 1.941 partidos, 142 equipos y 3.618 IDs de jugador |
| Consumidor v2 | Lector independiente, API, interfaz, explicación determinista y replay guardado implementados |
| Reprise | Código en la rama común y respaldo privado fijado por el puntero del consumidor |

Se conservan los datos desconocidos, ocho referencias entrantes sin resolver,
seis listas de sustituciones desconocidas, 15 jugadores sin metadatos y siete
conflictos de resultado. No se deducen estadísticas actuales de estas ediciones.
La regresión completa anterior del productor pasó 632 casos con una omisión;
el recheck de contratos históricos de cierre pasó 91. No se suman pruebas
solapadas como casos únicos.

Próximo trabajo: resultados prospectivos posteriores, evaluación humana y
aceptación DE/GenAI. El registro tiene 215 decisiones sobre 108 partidos, sin
resultados elegibles puntuados. Las correcciones de la fuente quedan auditadas.
La consulta final usó seis solicitudes de fútbol y 15 de PandaScore; cero filas
canónicas cambiadas. Consultar el informe de reprise para horas y huellas.

Solo fuentes gratuitas. No instalar, descargar, arrancar ni probar Ollama.
Docker no ofrecía motor; Spark de un millón, reinicio real, SLO sostenido,
Databricks autenticado y aceptación de instalación/release siguen pendientes.
