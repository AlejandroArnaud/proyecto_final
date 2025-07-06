# Análisis y Diseño del Esquema de Base de Datos y Proceso ETL para el Dataset OULAD

## 1. Introducción

Este documento detalla el análisis, diseño e implementación del modelo de datos y del proceso de Extracción, Transformación y Carga (ETL) para el dataset de Analíticas de Aprendizaje de la Open University (OULAD). El objetivo es construir una base de datos robusta, íntegra y optimizada para el análisis exploratorio de datos (EDA) y la modelización predictiva.

El diseño se basa en el esquema oficial del dataset, complementado con un análisis de las reglas de negocio implícitas en la descripción de los datos para crear un pipeline de ingesta de datos inteligente y resiliente.

## 2. Descripción Detallada del Dataset

A continuación, se presenta el contenido y la descripción de cada uno de los archivos que componen el dataset OULAD.

### 2.1. `courses.csv`
Contiene la lista de todos los módulos disponibles y sus presentaciones.
- **`code_module`**: Código identificador del módulo.
- **`code_presentation`**: Código de la presentación (semestre), compuesto por el año y una letra ('B' para inicio en febrero, 'J' para inicio en octubre).
- **`length`**: Duración de la presentación del módulo en días.

### 2.2. `assessments.csv`
Contiene información sobre las evaluaciones dentro de cada presentación de módulo.
- **`id_assessment`**: Número de identificación de la evaluación.
- **`assessment_type`**: Tipo de evaluación: TMA (Tutor Marked Assessment), CMA (Computer Marked Assessment) y Exam (Examen Final).
- **`date`**: Fecha de entrega final de la evaluación, calculada como el número de días desde el inicio de la presentación.
- **`weight`**: Ponderación de la evaluación en porcentaje.

### 2.3. `vle.csv`
Contiene información sobre los materiales disponibles en el Entorno Virtual de Aprendizaje (VLE).
- **`id_site`**: Número de identificación del material.
- **`activity_type`**: El rol asociado con el material del módulo.
- **`week_from` / `week_to`**: Rango de semanas en las que el material está planificado para ser utilizado.

### 2.4. `studentInfo.csv`
Contiene información demográfica de los estudiantes junto con sus resultados finales.
- **`id_student`**: Número de identificación único del estudiante.
- **`gender`**: Género del estudiante.
- **`region`**: Región geográfica donde vivía el estudiante.
- **`highest_education`**: Nivel educativo más alto del estudiante al ingresar a la presentación del módulo.
- **`imd_band`**: Banda del Índice de Múltiple Privación del lugar de residencia del estudiante.
- **`age_band`**: Rango de edad del estudiante.
- **`num_of_prev_attempts`**: El número de veces que el estudiante ha intentado este módulo.
- **`studied_credits`**: El número total de créditos que el estudiante está cursando actualmente.
- **`disability`**: Indica si el estudiante ha declarado una discapacidad.
- **`final_result`**: Resultado final del estudiante en la presentación del módulo.

### 2.5. `studentRegistration.csv`
Contiene información sobre el momento en que el estudiante se registró en la presentación del módulo.
- **`date_registration`**: Fecha de registro, medida en días relativos al inicio de la presentación (valores negativos indican registro anticipado).
- **`date_unregistration`**: Fecha de baja. Los estudiantes que completaron el curso tienen este campo vacío.

### 2.6. `studentAssessment.csv`
Contiene los resultados de las evaluaciones de los estudiantes.
- **`date_submitted`**: Fecha de entrega, medida en días desde el inicio de la presentación.
- **`is_banked`**: Indicador de que el resultado de la evaluación ha sido transferido de una presentación anterior.
- **`score`**: Puntuación del estudiante en la evaluación (rango 0-100).

### 2.7. `studentVle.csv`
Contiene información sobre las interacciones de cada estudiante con los materiales en el VLE.
- **`id_site`**: Número de identificación del material del VLE.
- **`date`**: Fecha de la interacción del estudiante.
- **`sum_click`**: Número de veces que un estudiante interactúa con el material en ese día.

## 3. Diseño del Esquema de la Base de Datos

### 3.1. Paradigma del Modelo: Instantánea de Datos (Snapshot Model)
El esquema implementado se basa en un modelo de "instantánea", dictado por la estructura del archivo `studentInfo.csv`. En este modelo, la tabla `studentInfo` no representa una entidad única de estudiante, sino la "fotografía" de sus datos demográficos y de rendimiento en el momento de una inscripción específica. Este diseño, aunque introduce redundancia, está optimizado para el análisis al mantener el contexto completo de cada registro.

### 3.2. Estructura de Tablas
La estructura de tablas se adhiere al diagrama oficial, con una mejora clave en la tabla `studentVle` y la adición de un campo de dominio en `studentAssessment`.

- **`courses`**: PK `(code_module, code_presentation)`
- **`studentInfo`**: PK `(id_student, code_module, code_presentation)`; FK a `courses`.
- **`studentRegistration`**: PK `(id_student, code_module, code_presentation)`; FK a `studentInfo`.
- **`assessments`**: PK `id_assessment`; FK a `courses`.
- **`vle`**: PK `id_site`; FK a `courses`.
- **`studentAssessment`**: PK `(id_assessment, id_student)`; FK a `assessments`. Contiene una columna adicional `assessment_result` para el dominio 'Pass'/'Fail'.
- **`studentVle`**: PK `id_interaction` (llave subrogada auto-incremental); FK a `studentRegistration` y `vle`.

## 4. Conclusiones del Análisis de Diseño y ETL

El análisis detallado de la descripción de los datos reveló reglas de negocio críticas que deben ser manejadas durante el proceso ETL para garantizar la calidad y completitud de los datos.

1.  **Imputación de Datos Guiada por Reglas:** La descripción de `assessments.csv` especifica que si la fecha de un examen final (`Exam`) es nula, esta debe corresponder al final de la presentación del curso. El proceso ETL debe implementar esta lógica, utilizando la `length` de la tabla `courses` para imputar estos valores faltantes, enriqueciendo así la completitud de los datos.

2.  **Creación de Campos de Dominio:** La regla de que una puntuación (`score`) inferior a 40 se interpreta como 'Fail' en `studentAssessment.csv` es una oportunidad para la creación de un campo de dominio. Se ha añadido una columna `assessment_result` a la tabla `studentAssessment`, que será poblada durante el ETL. Esto pre-procesa los datos y facilita análisis posteriores directamente desde la base de datos.

3.  **Necesidad de Llaves Subrogadas:** La tabla `studentVle` carece de una llave primaria natural y única, ya que un estudiante puede interactuar con el mismo recurso múltiples veces. Para normalizar y asegurar la unicidad de cada registro de interacción, se ha introducido una llave primaria subrogada auto-incremental (`id_interaction`).

4.  **Confirmación del Modelo de Instantánea:** La presencia de `code_module` y `code_presentation` en `studentInfo.csv` confirma que cada fila representa una inscripción, no un estudiante. El diseño de la base de datos y el ETL deben respetar esta estructura para preservar la integridad contextual de los datos.