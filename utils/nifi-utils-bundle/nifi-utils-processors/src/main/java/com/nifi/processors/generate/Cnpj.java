package com.nifi.processors.generate;

import org.apache.commons.lang3.StringUtils;

import java.util.Random;

/**
 * Created by fabiano on 09/03/17.
 */
public class Cnpj implements DocumentoReceita {

    private static final int LENGHT_CNPJ = 12;
    private static final int LENGHT_CNPJ_WITH_DF = 14;
    private static final int MASK_CNPJ = 99999999;
    private static final String HEAD_OFFICE = "0001";
    private static final int RULE_DF_MOD = 11;
    private static final int RULE_DF_MOD_DIF = 2;
    private static final int ZERO = 0;

    @Override
    public String generate(Integer estadoOrigem) {
        String generatedRandomNumber = generateRandomNumber();
        String firstDF = generateFirstDF(generatedRandomNumber);
        String secondDF = generateSecondDF(generatedRandomNumber.concat(firstDF));
        return generatedRandomNumber + firstDF + secondDF;
    }

    @Override
    public Boolean validate(String documentoReceita) {
        String documentoReceitaUnformated = format(documentoReceita, false);

        if (documentoReceitaUnformated.length() != LENGHT_CNPJ_WITH_DF)
            return false;

        String numberCnpj = documentoReceitaUnformated.substring(ZERO, LENGHT_CNPJ);
        Integer firstDF = Integer.parseInt(documentoReceitaUnformated.substring(LENGHT_CNPJ, LENGHT_CNPJ_WITH_DF - 1));
        Integer secondDF = Integer.parseInt(documentoReceitaUnformated.substring(LENGHT_CNPJ_WITH_DF - 1, LENGHT_CNPJ_WITH_DF));

        if (firstDF != Integer.parseInt(generateFirstDF(numberCnpj)) || secondDF != Integer.parseInt(generateSecondDF(numberCnpj + firstDF)))
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

    private String generateRandomNumber() {
        Integer num = new Random().nextInt(MASK_CNPJ);
        return StringUtils.leftPad(num.toString() + HEAD_OFFICE, LENGHT_CNPJ,  "0");
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
        Integer numberLength = number.length();
        Integer firstWeight = numberLength - 8;
        char[] num = number.toCharArray();

        for (int i = 0; i < numberLength; i++)
            if (i < firstWeight)
                sum += Integer.parseInt(String.valueOf(num[i])) * (firstWeight + 1 - i);
            else
                sum += Integer.parseInt(String.valueOf(num[i])) * (numberLength + 1 - i);

        Integer mod = sum% RULE_DF_MOD;

        if (mod > RULE_DF_MOD_DIF)
            df = RULE_DF_MOD - mod;

        return df.toString();
    }

    private String formatWithMask(String documentoReceita) {
        String documentoReceitaUnformated = format(documentoReceita, false);
        return documentoReceitaUnformated.replaceAll("(\\d{2})(\\d{3})(\\d{3})(\\d{4})(\\d{2})", "$1.$2.$3/$4-$5");
    }

    private String formatWithoutMask(String documentoReceita) {
        return documentoReceita.replaceAll("[^0-9]", "");
    }

}
