package com.nifi.processors.generate;

/**
 * Created by fabiano on 09/03/17.
 */
public class Cnpj implements DocumentoReceita {

    @Override
    public String generate(Integer estadoOrigem) {
        return null;
    }

    @Override
    public Boolean validate(String documentoReceita) {
        return null;
    }

    @Override
    public String format(String documentoReceita) {
        return null;
    }
}
