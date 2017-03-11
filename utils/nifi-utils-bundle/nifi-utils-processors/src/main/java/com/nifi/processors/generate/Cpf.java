package com.nifi.processors.generate;

import com.auth0.jwt.internal.org.apache.commons.lang3.StringUtils;

import java.util.Random;

/**
 * Created by fabiano on 09/03/17.
 */
public class Cpf implements DocumentoReceita {

    private static int LENGHT_CPF = 9;
//    private static int LENGHT_DIGITO_VERIFICADO = 2;
    private static int MASK_CPF_WITHOUT_ESTADO_ORIGEM = 999999999;
    private static int MASK_CPF_WITH_ESTADO_ORIGEM = 99999999;

    @Override
    public String generate(Integer estadoOrigem) {
        String numeroAleatorio = "";

        while (!validateGeneratedNumber(numeroAleatorio)) {
            if (estadoOrigem != null && validateEstadoOrigem(estadoOrigem))
                numeroAleatorio = generateRandomNumberWithEstadoOrigem(estadoOrigem);
            else
                numeroAleatorio = generateRandomNumberWithoutEstadoOrigem();
        }
        //TODO Fazer a lógica do DF

        return numeroAleatorio;
    }

    @Override
    public Boolean validate(String documentoReceita) {
        return null;
    }

    @Override
    public String format(String documentoReceita) {
        String newValue = null;
        if (documentoReceita != null) {
            String cpf_1 = documentoReceita.substring(0, 3);
            String cpf_2 = documentoReceita.substring(3, 6);
            String cpf_3 = documentoReceita.substring(6, 9);
            String cpf_4 = documentoReceita.substring(9);
            newValue = cpf_1 + "." + cpf_2 + "." + cpf_3 + "-" + cpf_4;
        }
        return newValue;

        //TODO Talvez incluir o replace de qualquer caractere não numérico
    }

    private Boolean validateEstadoOrigem(Integer estadoOrigem) {
        return (estadoOrigem >=0 && estadoOrigem <=9) ? true : false;
    }

    private String generateRandomNumberWithoutEstadoOrigem() {
        Integer num = new Random().nextInt(MASK_CPF_WITHOUT_ESTADO_ORIGEM);
        return StringUtils.leftPad(num.toString(), LENGHT_CPF,  "0");
    }

    private String generateRandomNumberWithEstadoOrigem(Integer estadoOrigem) {
        Integer num = new Random().nextInt(MASK_CPF_WITH_ESTADO_ORIGEM);
        return StringUtils.leftPad(num.toString() + estadoOrigem, LENGHT_CPF,  "0");
    }

    private Boolean validateGeneratedNumber(String number) {
        if (StringUtils.isEmpty(number))
            return false;
        else {
            int countFirstChar = StringUtils.countMatches(number, number.substring(0,1));
            return (countFirstChar == number.length()) ? false : true;
        }
    }

    public static void main(String[] args) {
        Cpf cpf = new Cpf();
        System.out.println(cpf.generate(9));
    }

}