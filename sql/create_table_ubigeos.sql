CREATE TABLE ubigeos
(
    codigo      text,
    pariente    text,
    descripcion text,
    extranjero  boolean,
    PRIMARY KEY (codigo),
    FOREIGN KEY (pariente) REFERENCES ubigeos (codigo)
);