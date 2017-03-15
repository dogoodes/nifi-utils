package com.nifi.processors.generate;

import org.apache.commons.lang3.StringUtils;

import java.util.Random;

/**
 * Created by fabiano on 09/03/17.
 */
public class Cpf implements DocumentoReceita {

    private static final int LENGHT_CPF = 9;
    private static final int LENGHT_CPF_WITH_DF = 11;
    private static final int MASK_CPF_WITHOUT_ESTADO_ORIGEM = 999999999;
    private static final int MASK_CPF_WITH_ESTADO_ORIGEM = 99999999;
    private static final int RULE_DF_MOD = 11;
    private static final int RULE_DF_MOD_DIF = 2;
    private static final int ZERO = 0;

    @Override
    public String generate(Integer estadoOrigem) {
        String generatedRandomNumber = "";

        while (!validateGeneratedNumber(generatedRandomNumber)) {
            if (estadoOrigem != null && validateEstadoOrigem(estadoOrigem))
                generatedRandomNumber = generateRandomNumberWithEstadoOrigem(estadoOrigem);
            else
                generatedRandomNumber = generateRandomNumberWithoutEstadoOrigem();
        }

        String firstDF = generateFirstDF(generatedRandomNumber);
        String secondDF = generateSecondDF(generatedRandomNumber.concat(firstDF));

        return generatedRandomNumber + firstDF + secondDF;
    }

    @Override
    public Boolean validate(String documentoReceita) {
        String documentoReceitaUnformated = format(documentoReceita, false);

        if (documentoReceitaUnformated.length() != LENGHT_CPF_WITH_DF)
            return false;

        String numberCpf = documentoReceitaUnformated.substring(ZERO, LENGHT_CPF);
        Integer firstDF = Integer.parseInt(documentoReceitaUnformated.substring(LENGHT_CPF, LENGHT_CPF_WITH_DF - 1));
        Integer secondDF = Integer.parseInt(documentoReceitaUnformated.substring(LENGHT_CPF_WITH_DF - 1, LENGHT_CPF_WITH_DF));

        if (firstDF != Integer.parseInt(generateFirstDF(numberCpf)) || secondDF != Integer.parseInt(generateSecondDF(numberCpf + firstDF)))
            return false;

        return true;
    }

    @Override
    public String format(String documentoReceita, Boolean withMask) {
        String newValue = "";

        if (documentoReceita != null) {
            if (withMask)
                newValue = formatWithMask(documentoReceita);
            else
                newValue = formatWithoutMask(documentoReceita);
        }

        return newValue;
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

    private String generateFirstDF(String number) {
        return ruleDF(number);
    }

    private String generateSecondDF(String number) {
        return ruleDF(number);
    }

    private String ruleDF(String number) {
        Integer sum = new Integer(ZERO);
        Integer df = new Integer(ZERO);
        char[] num = number.toCharArray();

        for (int i = 0; i < number.length(); i++)
            sum += Integer.parseInt(String.valueOf(num[i])) * (number.length() + 1 - i);

        Integer mod = sum%RULE_DF_MOD;

        if (mod > RULE_DF_MOD_DIF)
            df = RULE_DF_MOD - mod;

        return df.toString();
    }

    private String formatWithMask(String documentoReceita) {
        String documentoReceitaUnformated = format(documentoReceita, false);
        return documentoReceitaUnformated.replaceAll("(\\d{3})(\\d{3})(\\d{3})(\\d{2})", "$1.$2.$3-$4");
    }

    private String formatWithoutMask(String documentoReceita) {
        return documentoReceita.replaceAll("[^0-9]", "");
    }

}