import regex

c_type_beg = r"[\s\.,-]*(?<![\b\w\.])"
c_type_end = r"[\s\.,-]*(?=[\s\.,-]*)(?![\w\.])"
soc_merc = {
    r"s(oc(iedad)?)?.l(im(itada)?)?": "s.l",
    r"s(oc(iedad)?)?.l(im(itada)?)?.l(ab(oral)?)?": "s.l.l",
    r"s(oc(iedad)?)?.l(im(itada)?)?.u(nipersonal)?": "s.l.u",
    r"s(oc(iedad)?)?.l(im(itada)?)?.p(rof(esional)?)?": "s.l.p",
    r"s(oc(iedad)?)?.l(im(itada)?)?.n(ueva)?.e(mp(resa)?)?": "s.l.n.e",
    r"s(oc(iedad)?)?.(de )?r(es(p(onsabilidad)?)?)?.l(im(itada)?)?": "s.r.l",
    r"s(oc(iedad)?)?.a(n(o|ó)nima)?": "s.a",
    r"s(oc(iedad)?)?.a(n(o|ó)nima)?.l(ab(oral)?)?": "s.a.l",
    r"s(oc(iedad)?)?.a(n(o|ó)nima)?.u(nipersonal)?": "s.a.u",
    r"s(oc(iedad)?)?.a(n(o|ó)nima)?.d(ep(ortiva)?)?": "s.a.d",
    r"s(oc(iedad)?)?.a(n(o|ó)nima)?.e(s(p(añola)?)?)?": "s.a.e",
    r"s(oc(iedad)?)?.c(iv(il)?)?": "s.c",
    r"s(oc(iedad)?)?.c(iv(il)?)?.p(riv(ada)?)?": "s.c.p",
    r"s(oc(iedad)?)?.c(iv(il)?)?.p(art(icular)?)?": "s.c.p",
    # r"s(oc(iedad)?)?.c(oop(erativa)?)?":'c.o.o.p',
    r"(s(oc(iedad)?)?)?.c.o.o.p(erativa)?": "c.o.o.p",
    r"s(oc(iedad)?)?.c(oop(erativa)?)?.a(ndaluza)?": "s.c.a",
    r"s(oc(iedad)?)?.c(oop(erativa)?)?.l(im(itada)?)?": "s.c.l",
    r"s(oc(iedad)?)?.c(oop(erativa)?)?.i(nd(ustrial)?)?": "s.c.i",
    r"s(oc(iedad)?)?.a(graria)?.(de )?t(ransformaci(o|ó)n)?": "s.a.t",
    r"s(oc(iedad)?)?.(de )?g(arant(i|í)a)?.r(ec(i|í)proca)?": "s.g.r",
    r"s(oc(iedad)?)?.i(rr(eg(ular)?)?)?": "s.i",
    r"s(oc(iedad)?)?.r(eg(ular)?)?.c(ol(ectiva)?)?": "s.r.c",
    r"s(uc(ursal)?)?.(en )?e(s(p(aña)?)?)?": "s.e.e",
    r"s(oc(iedad)?)?.(en )?c(om(andita)?)?": "s.e.n.c",
    r"c(om(unidad)?)?.(de )?b(ienes)?": "c.b",
    r"a(grupaci(o|ó)n)?.(de )?i(nt(er(e|é)s)?)?.e(con(o|ó)mico)?": "a.i.e",
    r"a(grupaci(o|ó)n)?.e(uropea)?.(de )?i(nt(er(e|é)s)?)?.e(con(o|ó)mico)?": "a.e.i.e",
    r"u(ni(o|ó)?n)?.t(emp(oral)?)?.(de )?e(mp(resas)?)?": "u.t.e",
    r"inc(orporated)?.": "inc",
    r"l(imi)?t(e)?d.": "ltd",
    r"co": "co",
    # r"llc": "llc",
}


def replace_company_types(text, remove_type=False):
    """
    Replace the company type if present in a text in any given format
    (e.g.: "s.l.", "sl", "s. l.") into a standard form ("s.l.")
    or remove it if `remove_type`=`True`.
    """
    for pat, soc in soc_merc.items():
        pat = pat.replace(".", r"[\s\.]*")
        pattern = rf"{c_type_beg}{pat}{c_type_end}"
        if remove_type:
            soc = " "
        else:
            soc = " " + soc + ". "
        text = regex.sub(pattern, soc, text)
    text = regex.sub(r"\s+", " ", text).strip()
    return text
