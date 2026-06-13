# **Rúbrica propuesta — Primer Parcial**

**Escala:** 100 puntos \= nota 10\.  
**Nota final:** Nota \= puntaje / 10\.

| Criterio | Puntos | Qué se evalúa |
| ----- | ----- | ----- |
| **1\. Comprensión del problema de negocio y alcance del parcial** | **10** | Identifica correctamente que el caso es de un proveedor cloud orientado a FinOps, Soporte y Producto. Distingue real-time para métricas operativas y batch para maestros/facturación. No se desvía hacia un proyecto genérico de BI, ML o dashboards sin pipeline de datos. |
| **2\. Diagrama de arquitectura de alto nivel** | **20** | Presenta una arquitectura clara, legible y coherente. Debe incluir fuentes landing, ingesta batch, ingesta streaming, zonas de Data Lake, procesamiento, almacenamiento, capa Gold y serving. Debe indicar si el patrón elegido es Lambda o Kappa y mostrar el flujo general. |
| **3\. Elección y justificación del patrón arquitectónico** | **10** | Justifica correctamente Lambda o Kappa según el caso. Para este proyecto, una solución Lambda suele ser más natural porque hay batch de maestros/facturación y streaming de eventos, aunque Kappa también puede aceptarse si se argumenta bien. Se penaliza elegir un patrón sin explicar trade-offs. |
| **4\. Mapeo de requisitos a componentes** | **15** | Incluye una tabla o lista que relacione requisitos del proyecto con componentes concretos. Por ejemplo: Structured Streaming para eventos, Bronze Parquet para datos tipificados, quarantine para errores, Silver para limpieza/enriquecimiento, Gold para marts, Cassandra para serving query-first. Debe incluir las 5V del Big Data como pie del parcial. |
| **5\. Flujo de datos / Data Pipeline** | **15** | Describe el flujo desde fuentes hasta outputs analíticos. Debe distinguir batch vs streaming, mostrar etapas ETL/ELT, explicar cómo pasan los datos por Landing, Bronze, Silver y Gold, e indicar dónde se aplican validaciones, deduplicación, joins, agregaciones y carga a Cassandra. |
| **6\. Asunciones y riesgos iniciales** | **10** | Lista asunciones realistas y riesgos técnicos con mitigación. Ejemplos: latencia de streaming, datos tardíos, evolución de esquema, nulos, outliers, claves de BD mal diseñadas, volumen de datos, duplicados, problemas de conectividad, límites de Colab. No basta con mencionar riesgos genéricos; deben estar asociados al proyecto. |
| **7\. Estimación de esfuerzo y recursos** | **8** | Presenta una estimación razonable de tiempo, roles y recursos. Debe incluir al menos tareas principales, responsables o perfiles, y una idea de esfuerzo. No se espera planificación granular, pero sí una estimación creíble para diseñar, implementar y probar el pipeline. |
| **8\. Claridad, estructura visual y calidad académica del entregable** | **7** | Documento ordenado, fácil de leer, con diagramas comprensibles, terminología correcta y redacción técnica. El diseño debe ser visual, accionable y no excesivamente extenso. Se penaliza texto desordenado, diagramas ilegibles o explicaciones superficiales. |
| **9\. Uso estimado de IA y evidencia de apropiación técnica** | **5** | Evalúa si la entrega parece construida críticamente por el estudiante o si es una salida genérica de IA sin dominio. No se penaliza automáticamente el uso de IA, pero sí la falta de criterio, personalización, consistencia y defensa técnica. |

**Total: 100 puntos**.

