import akvut

avsendar = "aasmund.kvamme@hvl.no"
mottakarar = ["aasmund.kvamme@hvl.no"]
tittel = "Test av e-post"
innhald = "Dette er ein test av e-post."
innhald += "\nDette er ein nytt linje."
vedlegg = ""

akvut.send_epost(tittel, innhald, avsendar, mottakarar, vedlegg)