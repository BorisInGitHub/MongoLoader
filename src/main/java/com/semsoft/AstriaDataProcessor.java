package com.semsoft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Cas particulier pour Astria
 * Created by breynard on 09/05/17.
 */
public class AstriaDataProcessor implements DataProcessor {
    List<String> columnsHeaders = Arrays.asList(
            "NUMCAU", "EWEBDOS", "TYPLOCAPAS", "CAUINTITU", "CODTYPCAU",
            "CAUTYPCNV", "CAUCOBEN", "CAUCTLPIE", "CODLOCDOS", "CODORICAU",
            "REFAPP", "ENUMDOS", "IDOPAS", "DOSELIG", "DARRDOS", "DDOSCMP",
            "DATOUVDOS", "DOUVCAU", "DOUVDEP", "DATACCCAU", "DMELOUV", "DMELBOUV",
            "DMELATT", "DMELBATT", "DMELENGD", "DMELBENGD", "DMELENGC", "DMELBENGC",
            "DMELBADEXP", "DMELBAPNR", "COMPAS01", "COMPAS02", "COMPAS03", "COMPAS04",
            "CODDECIPAS", "DATDECI", "DACCPAS", "NUMCOLOUV", "NUMCOLINS", "NUMCOLGES",
            "DATGES", "NUMCOLCTX", "CODCTX", "DTRACTX", "CODREVPAS", "DREVDOS", "NUMCOLACC",
            "DENVPAS", "CODANNPAS", "DANNPAS", "NUMCOLAPAS", "CODCLOPAS", "DCLOPAS", "NUMCOLCLO",
            "REFDOSBAI", "NUMCAUBIS", "CODETAPAS", "DETAPAS", "CODETAPASP", "DETAPASP", "NUMCOLC",
            "DATCREC", "NUMCOLM", "DATMODC", "CAUPPH1", "NOMCAU", "NUMENT1", "REFEMP1", "BOOPLABES",
            "BOOSUREND", "DSUREND", "CODPOSBEN", "DDPTBEN", "BOOSWAP", "DSWAP", "CAUPPH2", "NOMCOBEN",
            "NUMENT2", "REFEMP2", "CODPOSCBEN", "DDPTCBE", "MNTDEPOT", "MNTDEPBES", "CODNATDEP", "CODDESPAI",
            "BOOPAIDEP", "CODTYPOFF", "DOFRPAS", "DLIMPAS", "BOOACCOFR", "DACCOFR", "DRETPAS", "BOOEDOFR",
            "DATREDOFR", "DMADPRV", "DPAIDEP", "MNTDEPOFR", "TOTPAIDEP", "DERPAIDEP", "DEPETA", "DEPAGE",
            "DEPSER", "DEPSOUSER", "DEPDOS", "DEPDOSPRE", "DEPJOUECH", "DEPD1ECH", "DEPDECH", "DEPCODPER",
            "DEPNBECH", "DEPMNTECH", "BOOTRTAMO", "BOOREVDEP", "BOOCTXDEP", "CODETADEP", "DETADEP", "CODETADEPP",
            "DETADEPP", "CODANNDEP", "DANNDEP", "NUMCOLADEP", "DDEPBEN", "BOODEPEFF", "DMELBAI", "DLOCTDL",
            "DMELLOC", "DLOCTD1", "MNTRBTDEP", "DRBTDEP", "MNTPERDEP", "DPERDEP", "DERPAIPER", "CODPERTE", "MNTRESTI",
            "DRESTI", "DERPAIRBT", "MNTFRADEP", "REFDEPORI", "ANCPREDEP", "DENVPTT", "DSIGNBAI", "DENGCAU", "NUMCOLCAU",
            "DDEBCAU", "DFINCAU", "DPROROGCAU", "NBMOISCAU", "MNTENGCAU", "MNTCAUACT", "DACTCAU", "MNTUTI", "NBMOISUTI",
            "TOTRBTCAU", "MNTLOYCAU", "MNTPERCAU", "DPERCAU", "CODPERCAU", "DERCAUPER", "MNTFRACAU", "MRESTICAU",
            "DRESTICAU", "DERPAICAU", "CAUETA", "CAUAGE", "CAUSER", "CAUSOUSER", "CAUDOS", "BOOREVCAU", "BOOCTXCAU",
            "CODANNCAU", "DANNCAU", "NUMCOLACAU", "DEXPCAU", "CODETACAU", "DETACAU", "CODETACAUP", "DETACAUP",
            "REFCAUORI", "REFBAI", "REFGES", "ANCREFBAI", "PREFXBAI", "NOMINTBAI", "PRENOMINTB", "ADR1INTBAI",
            "ADR2INTBAI", "POSINTBAI", "VILINTBAI", "TELINTBAI", "FAXINTBAI", "EMAILBAI", "CODRSCINT", "RSCINTBAI",
            "DEFFBAI", "BOORETBAIL", "DRETBAI", "DRELBAI", "DRESBAI", "NUMLOG", "NUMLOGGER", "CODTYPLOG", "CODNATLOG",
            "ADR1LOG", "ADR2LOG", "POSLOG", "VILLOG", "CODTYPPARC", "DATENTP", "SURLOG", "MNTLOY", "MNTCHA", "MNTPKG",
            "MNTLOYT", "MNTLOYBES", "NUMCAN", "CODZON", "MNTLOYM2", "MNTMAXRES", "REFUESLDE", "BENDEPUESL", "ANNGARDE",
            "TRIGARDE", "MNTUESLDE", "ANNEXPIDE", "TRIEXPIDE", "ANNANNDEP", "TRIANNDEP", "ANNRBTDEP", "TRIRBTDEP",
            "RBTUESLDE", "PERTUESLDE", "REFUESLCA", "BENCAUUESL", "ANNGARCA", "TRIGARCA", "MNTUESLCA", "ANNEXPICA",
            "TRIEXPICA", "ANNAJU", "TRIAJU", "ANNEXPCAU", "TRIEXPCAU", "NBMOIUESL", "MNTUESL", "BENMJEUESL",
            "MNTMJEUESL", "ANNDERMJE", "TRIDERMJE", "ANNANNMJE", "TRIANNMJE", "MJEANNUESL", "RBTUESLCAU",
            "PERUESLCAU", "CPTRDEP", "CPTRCAU", "DEP2UESL", "ADETAPAS", "BOOAPL", "BOOANAH", "IDCIL", "BOODOSCONF",
            "HCAUT", "DCAUT", "ADATE", "tcautDannpas", "caumDatpaie", "type_Pret_Aide", "MNTDEPOFR_PRE_TRAITE",
            "DOFRPAS_PRE_TRAITE", "DPAIDEP_PRE_TRAITE", "TOTPAIDEP_PRE_TRAITE", "DATE_ANNULATION_LOCAPASS",
            "MNTENGCAU_PRE_TRAITE", "DATACCCAU_PRE_TRAITE", "DANNCAU_PRE_TRAITE", "DATPAIE_PRE_TRAITE",
            "ID_AVANCES_LOCAPASS", "ID_GARANTIES_LOCAPASS", "actif_LocaPasA", "actif_LocaPasG",
            "actif_LocaPasGeneral", "statutAvancement_LocaPassGeneral", "statutAvancement_LocaPassA",
            "statutAvancement_LocaPassG", "etat_LocaPassGeneral", "etat_LocaPassA", "etat_LocaPassG",
            "dateDebut", "Nom famille produit", "BANQUE", "RIB", "ADRESSE_CONCAT", "IDENTIFIANT_ETABLISSEMENT",
            "IDENTIFIANT_SERVICE", "MTPAY_PRE_TRAITE");
    List<String> columnsToKeep = Arrays.asList(
            "NUMCAU", "NUMCOLGES", "CAUPPH1", "CAUPPH2", "POSLOG", "VILLOG", "type_Pret_Aide", "actif_LocaPasA", "actif_LocaPasG",
            "actif_LocaPasGeneral", "statutAvancement_LocaPassGeneral", "statutAvancement_LocaPassA", "statutAvancement_LocaPassG",
            "dateDebut", "ADRESSE_CONCAT", "IDENTIFIANT_ETABLISSEMENT", "ID_AVANCES_LOCAPASS", "ID_GARANTIES_LOCAPASS",
            "MNTDEPOFR_PRE_TRAITE", "DOFRPAS_PRE_TRAITE", "DPAIDEP_PRE_TRAITE", "TOTPAIDEP_PRE_TRAITE",
            "DATE_ANNULATION_LOCAPASS", "MNTENGCAU_PRE_TRAITE", "DATACCCAU_PRE_TRAITE",
            "DANNCAU_PRE_TRAITE", "DATPAIE_PRE_TRAITE", "MTPAY_PRE_TRAITE"
    );
    List<String> columnsWithIndexes = Arrays.asList(
            "NUMCAU", "NUMCOLGES", "CAUPPH1", "CAUPPH2", "IDENTIFIANT_ETABLISSEMENT"
    );


    @Override
    public List<String> keepUsedColumns(List<String> row) {
        List<String> result = new ArrayList<>(columnsToKeep.size());

        for (String header : columnsToKeep) {
            int i = columnsHeaders.indexOf(header);
            String data = row.get(i);
            if (data != null) {
                result.add(data);
            } else {
                result.add(null);
            }
        }
        return result;
    }

    @Override
    public List<String> columsWithIndex() {
        List<String> result = new ArrayList<>(columnsWithIndexes.size());

        for (String header : columnsWithIndexes) {
            int i = columnsToKeep.indexOf(header);
            result.add(Integer.toString(i));
        }
        return result;
    }
}
