# Retomar Arena en el otro PC — 7 de octubre de 2026

Punto de entrada actual para ambos repositorios, rama `checkpoint/2026-10-07`.
Este checkpoint cierra una etapa; la hoja de ruta completa sigue abierta.
Las notas anteriores de NEXT_SESSION/HANDOFF conservan sus fechas originales.

## Qué quedó terminado

| Aplicación | Avance | Evidencia |
| --- | --- | --- |
| arena-lakehouse | Monitor de salud, metadatos ligeros y presupuesto Spark configurable | Ruff y suites acotadas; informes fechados |
| arena-lakehouse | Preflight de identidad y Spark/Delta nativo con 10.000 observaciones | Paridad de todas las filas con Python y SQLite; replay, rechazo y recuperación |
| arena-lakehouse | Paquete privado de detalles ligado a publicación y archivos fuente | Pruebas del productor, conflictos y capturas reales |
| lakehouse-copilot | Vista compilada sin Vite permanente, presupuesto de consultas y preguntas ligadas a fuente | Compilación TypeScript/Vite y pruebas de navegación |
| lakehouse-copilot | Investigaciones guardadas y revisión manual de la respuesta exacta | 21 pruebas backend y un skip Windows; pruebas de escritorio/móvil |
| lakehouse-copilot | Contexto del partido, mapas VALORANT y pestañas de equipos/alineaciones/jugadores | Contrato, vistas reales a dos anchuras y 10 pruebas independientes finales |

Current local conserva 2.132 partidos de fútbol, 106 partidos canónicos de
VALORANT y 122 registros de calendario PandaScore. El catálogo local contiene
180 logotipos. Cuatro vistas reales verificaron sus dos escudos, sin errores
ni desbordamiento; no se comprobó cada URL del catálogo.

**Datos pendientes:** las capturas gratuitas no aportaron alineaciones,
estadísticas de equipo ni contadores individuales. Pedir alineaciones desplegadas
en los seis requests existentes tampoco las obtuvo. Los mapas muestran ganador
y duración cuando la fuente los aporta. Kills, KDA, ACS, ADR, nombres de mapas y
rondas desconocidos no se inventan. Los jugadores de prueba son fixtures.

## Recuperar el código conservando el trabajo del otro PC

En cada carpeta existente, comprobar primero que no hay cambios sin guardar:

```powershell
git status --short
git fetch origin
git switch checkpoint/2026-10-07
git pull --ff-only origin checkpoint/2026-10-07
```

Si hay cambios locales o divergencias, conservar primero el trabajo en un commit
propio y revisar ambas historias. No usar reset ni checkout/push forzado.
Si las carpetas no existen, clonar ambas ramas como proyectos vecinos:

```powershell
git clone --branch checkpoint/2026-10-07 https://github.com/Manuelma4/arena-lakehouse.git
git clone --branch checkpoint/2026-10-07 https://github.com/Manuelma4/lakehouse-copilot.git
```

## Abrir mañana sin Docker ni modelos

Desde la carpeta padre, si todavía no existe el entorno compartido:

```powershell
py -3.12 -m venv .arena-venv
.\.arena-venv\Scripts\python.exe -m pip install -e "./arena-lakehouse[dev]" -e "./lakehouse-copilot[dev]"
Set-Location lakehouse-copilot
.\start-demo.ps1 -ProductionPreview -OpenBrowser
```

Python 3.11 o 3.13 también son compatibles; Node.js 24 debe estar instalado.
Conservar el entorno existente si ya funciona. En Windows usar `npm.cmd`;
el launcher instala dependencias web si faltan. Detener una instancia anterior
con `stop-demo.ps1` antes de iniciar otra.

URL: **http://127.0.0.1:8000**, salud: **/health**, documentación: **/docs**.
La vista compilada se reconstruye al arrancar; omitir ProductionPreview permite
hot reload. El launcher espera API verificada y web disponible, y limpia sus
procesos si el arranque falla.

Conservar el .env del otro PC. Si falta, copiar .env.example a .env y completar
valores localmente. Las credenciales pertenecen solo al productor. Ajustar:

- ARENA_LIVE_WORKSPACE: ruta real de `arena-lakehouse/data/live`.
- ARENA_STATE: estado local del consumidor.
- ARENA_ENABLE_LOCAL_TEAM_MEDIA=true y ARENA_TEAM_MEDIA: ruta del catálogo
  `lakehouse-copilot/data/team-media.json`, si se transfirió.
- ARENA_QUERY_THREADS=1, ARENA_QUERY_MEMORY_MB=256, ARENA_QUERY_CONCURRENCY=2.

Estos presupuestos no limitan toda la RAM del proceso. No se cambió la asignación
global Docker/WSL. La aplicación carga su .env; el aviso VS Code sobre
`python.terminal.useEnvFile` afecta terminales interactivas y es opcional para
ese uso, no un requisito del launcher.

## Transportar también Current e investigaciones

Git conserva código y planes, no .env, datos privados, SQLite, dependencias ni
modelos. En este PC quedó la copia privada verificada:

`lakehouse-copilot/data/arena-private-handoff-20261007-final.zip`

97 archivos, 15 backups SQLite mediante su API, 1.746.671 bytes; SHA256:

`294154ce4eae74f945ce285b7404a46135a3b22b3b2d40bd27520be74d834e90`

Se verificó integridad y ausencia de los tokens locales. Excluye .env, locks,
WAL/SHM, PID del launcher y configuración Docker. Solo el ZIP **final** está
verificado; el primer intento quedó como artefacto local tras fallar su limpieza.

Copiarlo de forma privada al otro PC, sin subirlo al repositorio. Extraer en una
carpeta nueva y vacía, por ejemplo `arena-private-restore-20261007`, conservando
sus propios datos. Ajustar el .env a:

- ARENA_LIVE_WORKSPACE: `<restore>/arena-lakehouse/data/live`
- ARENA_STATE: `<restore>/lakehouse-copilot/data/copilot`
- ARENA_TEAM_MEDIA: `<restore>/lakehouse-copilot/data/team-media.json`

PRIVATE-HANDOFF.json contiene hashes por archivo. Las observaciones copiadas
pueden ser antiguas mañana y deben mostrarse con su fecha real. No iniciar un
segundo propietario de polling junto al colector alojado. Nuevas adquisiciones
locales requieren un ciclo explícito y acotado según las cuotas documentadas.

## Docker: fallo final de este equipo

Las pruebas nativas terminaron antes de un fallo posterior de reposo: timeout
al pausar, después HTTP 500/no connected VM, y WSL sin respuesta. Se desactivó
Resource Saver conservando memoria/CPU y un backup privado de configuración.
La terminación acotada de docker-desktop y el intento elevado de reiniciar WSL
vencieron sus timeouts. Último estado: WslService StopPending y Docker detenido.

La reparación sigue pendiente. No se reinició Windows, borraron volúmenes ni
aumentó memoria. Guardar trabajo y revisar tras un reinicio manual cuando el
usuario lo decida; verificar `docker version` antes de más Spark. No se garantiza
que el reinicio resuelva el fallo. Es un problema local y no debe asumirse
presente en el otro PC. Ver el informe docker-idle-recovery en docs/evidence.
No ejecutar automáticamente los scripts de reparación en otra máquina.

## Orden de trabajo y criterios de aceptación

1. **Fuente gratuita real.** Revisar documentación y permisos antes de integrar
   detalles fútbol/VALORANT. Medir cobertura por competición/temporada/partido.
   Exigir captura permitida real, IDs de partido/jugador, original y hash,
   fecha de observación y desconocidos explícitos. Si exige pago o falta permiso,
   registrar el bloqueo y avanzar otra tarea.
2. **Contrato del productor.** Solo tras la captura, versionar jugadores/mapas:
   v1 permite cero estadísticas individuales. Conservar publicación exacta,
   conflictos, identidades y compatibilidad. Replay idempotente y daño opcional
   deben dejar intacto el resultado canónico.
3. **Asistente e interfaz.** Añadir herramienta de lectura de estos detalles:
   las herramientas actuales todavía no consumen el paquete opcional. Citar
   fuente/partido y abstenerse al faltar datos. Verificar cambio de selección,
   respuestas tardías, móvil, teclado y aislamiento del modo público.
4. **Evaluación humana.** Revisar los 120 casos propuestos y congelar etiquetas
   antes de experimentar. Reportar soporte, exactitud numérica, abstención y
   claridad por modo. La pantalla permite revisión; las etiquetas automatizadas
   fueron interceptadas y no son evaluación humana ni juicios del usuario.
5. **Investigación interrumpida.** Implementar reanudación real de un checkpoint
   LangGraph incompleto. Reabrir respuestas completadas funciona; reanudar una
   ejecución incompleta sigue pendiente.
6. **Escala/operación.** Recuperar Docker antes de probar el millón de eventos
   con presupuesto adecuado y reinicio real de proceso. La aceptación 10k no
   garantiza un millón en 1536 MiB. La recuperación medida usó otra instancia
   en el mismo proceso/sesión Spark; no un reinicio del sistema.
7. **Release.** Backtesting/calibración con disponibilidad histórica real, SLO
   sostenido y Databricks autenticado siguen pendientes. Correr regresiones
   completas de la revisión final antes de merge/release/deploy. Las pruebas
   locales actuales son acotadas y mantienen sus fallos iniciales.

Restricciones vigentes: solo fuentes gratuitas; no instalar, descargar ni probar
Ollama sin una nueva instrucción; no aumentar memoria global, reiniciar Windows
ni borrar datos automáticamente. El checkpoint no publica cambios en main.
