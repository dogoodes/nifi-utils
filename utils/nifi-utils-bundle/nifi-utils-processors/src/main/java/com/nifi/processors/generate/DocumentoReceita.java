package com.nifi.processors.generate;

/**
 * Created by fabiano on 09/03/17.
 */
public interface DocumentoReceita {

    String generate(Integer estadoOrigem);

    Boolean validate(String documentoReceita);

    String format(String documentoReceita, Boolean withMask);

}
