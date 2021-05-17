# vacunas-covid-peru-datasette

[![Fetch data and deploy datasette](https://github.com/rmaceissoft/vacunas-covid-peru-datasette/actions/workflows/deploy.yml/badge.svg)](https://github.com/rmaceissoft/vacunas-covid-peru-datasette/actions/workflows/deploy.yml)

- Extrae los datos de los vacunados contra el covid-19 en el Perú del [portal de datos abiertos](https://www.datosabiertos.gob.pe/dataset/vacunaci%C3%B3n-contra-covid-19-ministerio-de-salud-minsa)
- Genera una base de datos SQLite usando [sqlite-utils](https://sqlite-utils.datasette.io) y [pandas](https://pandas.pydata.org/).
- Publica la base de datos usando [Datasette](https://datasette.io)


Desplegado en **heroku** en el siguiente enlace: https://vacunas-covid-peru.herokuapp.com/

## Fuentes de datos

- [Registro de vacunación nominal publicado por el MINSA en el portal de datos abiertos](https://www.datosabiertos.gob.pe/dataset/vacunaci%C3%B3n-contra-covid-19-ministerio-de-salud-minsa)
- [Indices de población y valores de latitud y longitud de los distritos del Perú reportados por CEPLAN y actualizados hasta el 2019](https://www.ceplan.gob.pe/download/224516/)
- [Códigos UBIGEO publicados por el INEI y disponibles en formato CSV a través de un repositorio del CONCYTEC](https://github.com/CONCYTEC/ubigeo-peru)