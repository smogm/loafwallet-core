//
//  BRPeerManager.c
//
//  Created by Aaron Voisine on 9/2/15.
//  Copyright (c) 2015 breadwallet LLC.
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.

#include "BRPeerManager.h"
#include "BRBloomFilter.h"
#include "BRSet.h"
#include "BRArray.h"
#include "BRInt.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <limits.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define PROTOCOL_TIMEOUT      20.0
#define MAX_CONNECT_FAILURES  20 // notify user of network problems after this many connect failures in a row
#define CHECKPOINT_COUNT      (sizeof(checkpoint_array)/sizeof(*checkpoint_array))
#define DNS_SEEDS_COUNT       (sizeof(dns_seeds)/sizeof(*dns_seeds))
#define GENESIS_BLOCK_HASH    (UInt256Reverse(u256_hex_decode(checkpoint_array[0].hash)))
#define PEER_FLAG_SYNCED      0x01
#define PEER_FLAG_NEEDSUPDATE 0x02

#if BITCOIN_TESTNET

static const struct {
    uint32_t height;
    const char *hash;
    uint32_t timestamp;
    uint32_t target;
} checkpoint_array[] = {
        {0, "4966625a4b2851d9fdee139e56211a0d88575f59ed816ff5e6a63deb4e3e29a0", 1486949366, 0x1e0ffff0}
};

static const char *dns_seeds[] = {
        "testnet-seed.ltc.xurious.com.",
        "seed-b.litecoin.loshan.co.uk.",
        "dnsseed-testnet.thrasher.io."
};

#else // main net

// blockchain checkpoints - these are also used as starting points for partial chain downloads, so they need to be at
// difficulty transition boundaries in order to verify the block difficulty at the immediately following transition
static const struct { uint32_t height; const char *hash; uint32_t timestamp; uint32_t target; } checkpoint_array[] = {
        {    0, "a60ac43c88dbc44b826cf315352a8a7b373d2af8b6e1c4c4a0638859c5e9ecd1", 1317972665, 0x1e0ffff0 },
        {   20160, "000000000d7e5ef4cf66cf7904f0c4904a7160ba81d234402e7541ea7c3827ca", 1395081446, 0x1c0e85b9},
        {   40320, "0000000002e229c0ef9fc2525b8b6c095bbb6228d7c7b544f7f6e6befe3bc696", 1396017526, 0x1c065c50},
        {   60480, "0000000002c38436fc16bca225ba485c9ce4bec0ceba4c9a9a623f79a1205ced", 1396973672, 0x1c0667c4},
        {   80640, "7d8c03c91a7b18fc6681785a775d015a7b4543160da725b6ace63d6143f9a8de", 1397921890, 0x1e0354ef},
        {   100800, "2f3ee64923cbc25157c039ab670a19c180e4a7c84917962e0800926e72aaf539", 1398819823, 0x1e01c653},
        {   120960, "565e2ac7cbba3adfbd9eab26d5f9ab91da16adfeca5812d6da566409053fa634", 1399754945, 0x1e01385e},
        {   141120, "99cb9a0860dd7529d6b1ef930085e7fd24d5b1727f3c17f1b0e4f8ae47272241", 1400643831, 0x1e00ebc2},
        {   161280, "249164c5e3ad8f1054cd466c748319e88ca229b1e867d09c67c45656ff4b8e1f", 1401579948, 0x1d162ebd},
        {   181440, "e3f18448f745c6a393007dd02b3da1f9644b4c79dc39fff742633f5c743d8618", 1402603738, 0x1d0cd669},
        {   201600, "80abb5d7cc03a46b56f9be8ff9b821a37411fa7c964aaa1e11283959010165b4", 1405407927, 0x1d0379a1},
        {   221760, "d649393bb035630cd8adabdaf09fc72e894ef682e7904f5da5a76e1be29dc8ec", 1409117657, 0x1d040d66},
        {   241920, "9665e2dda93e4cff6f3f7262a4ccf587ff7e08d1a06648a02c533ed81eba9558", 1412188896, 0x1d00b780},
        {   262080, "89da62d2a8512ad8fad06ef9d221626b788cb1b011288a360890ccdb5b670fdd", 1413479553, 0x1d4a8ae7},
        {   282240, "439b867a7a24334fb621770a86791e2aa5525369792606ae3d67881ae2cc967e", 1414437366, 0x1c0c97ea},
        {   302400, "4dd766afc92f08d8857d882d47e5a72f8a537809d4fa586e7b3045fa8c7c7b5e", 1415335746, 0x1d03337b},
        {   322560, "68ed6e32c19af45371c743b889f5a38285e64a90fde3ef42810973d645f493a5", 1416177421, 0x1d01aa2c},
        {   342720, "f36660d1ae5e0afb4c009f577f037aef8fd0a5f48e8161da28000c08c0deae7c", 1417059949, 0x1c345310},
        {   362880, "7f364ba0025f65f36ee717868e22fa19b717084801a69a9024c6dd1eb891a787", 1417914727, 0x1c39c7f0},
        {   383040, "576e79e9753e475e607dc49329390c921d527639d1504e5c633d53e0e44c422d", 1418766023, 0x1d00bbf5},
        {   403200, "344b7dc80eb7d301a5ed9bb12ea05d2a8846c6f300b5ffcfbe3ce30961649892", 1419644834, 0x1c5da030},
        {   423360, "2706e2dbffda46a22859cdfa8bf3f2bd52cc40ad98789869ba48fbc707e1d1ca", 1420527222, 0x1c0e1feb},
        {   443520, "ee06a2c20c427f6219c172b0ed34a1cdfba305eabfa7f6e9e2947c31c99d7519", 1421403987, 0x1c60ee4f},
        {   463680, "8ddb93418129bd6eaea8dbd1310574dca14b66beccb14c5dfd25910123a524f8", 1422299187, 0x1c545ef3},
        {   483840, "4c212b3a62c16a36c75f854a8268af09d032f8ab4060837d56f70f542904351d", 1423166080, 0x1c55e526},
        {   504000, "e19a376a4c2eb8aac730ce72e663b42e45b53fec7c9a9df77997e54f9f101f01", 1424056466, 0x1c58ce72},
        {   524160, "d7f4535d585778f31669b285fa7cee21d21aa7d8183d7026282177a0cddb5b22", 1424951677, 0x1c649eb7},
        {   544320, "f302eb430863372e6bbb46e3d29a2c35e7d4478ff4bf21fc11dabe0508392f2b", 1425838625, 0x1c324ff7},
        {   564480, "22d8e04cf01b6c99c148dc482bdcec6636bf2c25ae4e7ffa6562c8a881692496", 1426714412, 0x1c29dc4c},
        {   584640, "d722fa8bd9d4c89529ca6b8f5841d70ac5f8ab4ba4fab5b298ecac1a991b6c22", 1427576800, 0x1c6ebe18},
        {   604800, "86c0ebc17e66f3eeec6bee847528a6a30c8cf97a1b516f103dfbc65d118919df", 1428404967, 0x1c558f9d},
        {   624960, "58e13b21f4c24d4072b2f80f1cff2c6e7bed6afccb4286b4e75723b5c3fc1ed5", 1429262696, 0x1c716869},
        {   645120, "289a17e9ec491129a486230714006133e426c101086fbd0f63997635b2b36d1a", 1430120117, 0x1d0136bc},
        {   665280, "03aed295c899d7d0dd9ece77cfe0e0dacec893a0980c22dbfc92d4fa12b46bd2", 1430904669, 0x1c382b8b},
        {   685440, "d42a825562e57c31fa1730a049c96f8d1cabd68fff5fef5b56f6121ef2c8d871", 1431853226, 0x1d04a3b9},
        {   705600, "42104596d298b0b56cdda245ad4441c5c65821a8368aaf2714f550ece3f973d6", 1435308425, 0x1e0f7c27},
        {   725760, "83fa84cb8a88077343ffb41fd05d53ee524f9cf8ec1522eb1553b9d746c3da2f", 1437309030, 0x1e0fffff},
        {   745920, "3a455414fce585f3794fc53bb74a553fec973212f5e519cfebaab275ea5d32c9", 1439214387, 0x1e0fffff},
        {   766080, "30fdf3f1d7cf384368e317a440e936fa2cff2d694a981caba70183df6296738b", 1441152268, 0x1e0e534d},
        {   786240, "c9a4a5f7a088e820b6fa9bdb683ec81920f825559b436cdd89788a2ab5de79f4", 1442860170, 0x1e0fffff},
        {   806400, "d5b12373f985b33a314049f944982ffb594b1625f58be14c681e11835a1f3365", 1444829585, 0x1e0fffff},
        {   826560, "ae3a160e1d0188098cf84489513519b002434a372dc53aa22fac440f2acc5155", 1446915187, 0x1e0fffff},
        {   846720, "34a070d977ff26e153f8762f4c08789f9e900fdd659bc78686b0e8f48c14e7fa", 1449161501, 0x1e0fffff},
        {   866880, "9ded502490e8d87553cf0e9c4480a12e10d7c765d27df9602dd303174f78ca94", 1452085264, 0x1e0fffff},
        {   887040, "e4e52da579461eb8103c56d643518fd26c31ed04fd6e0b0c0bee62800defc528", 1454757897, 0x1e0f8a8c},
        {   907200, "bb8705bf59d7d47b6de3d9ae1dba0d916170e719ed9a1f4527a90a81bf57ec07", 1458296390, 0x1e0fffff},
        {   927360, "98369e8a5ab58771790faedd6929acb0c05802ed6f503fb91e0d34aec7827917", 1461132005, 0x1e0fffff},
        {   947520, "e89e9a2533905c2656850a4229a7fa02b19fc06a32a6f769df583d7323657fbf", 1464167122, 0x1e0e469a},
        {   967680, "8c1f484a534ada73a7347cd00a2b92c6dbe923a556c3071d2e09dc360d1c22e3", 1467658296, 0x1e0fffff},
        {   987840, "c0c4158ac4703b3e4e088f8e1f1b7dadd21d040504537305390ef307edf5e656", 1470872596, 0x1e0fffff},
        {   1008000, "49fb7c56f03e9bb8ba9927deb600a6913a7c3c0b6828173b5492c30d337c4177", 1475059353, 0x1e0fffff},
        {   1028160, "dddcb8dc02efa87a14f1a9d6305469fcf946dcffe3e8f80e234e8ed3e95955a0", 1477099431, 0x1d00a117},
        {   1048320, "6a24fd45c8af4169faff7b9d762d934eb09b59b11b201520fee7572223797a9e", 1478037263, 0x1c1b5135},
        {   1068480, "f26ab30f6ccf942dbab54dbd5e787308194de534091cad43a9ab3fb4841bc079", 1478940463, 0x1c5ce4c7},
        {   1088640, "017a3413e1da66bd059363c9b97102f1d6d6c032ec0a923a760e9ebedc18e404", 1479819871, 0x1d008587},
        {   1108800, "def7440f98f10cba4b5823a5fbf41889ff45b6e3dff82dd9721b817df1109f48", 1480718464, 0x1c7c970b},
        {   1128960, "a2d4ca74ff3ac78e2fee69cd7786c0408fd9d8f76e98380e6c1d5d5c124f35c0", 1481627259, 0x1c258383},
        {   1149120, "5c0c361ced054211e9fba9f04175d7211a98502d1a590de861323820a98c21bf", 1482538657, 0x1c29b93a},
        {   1169280, "5671e60f752bcbf4c22fe2cafc660085cc86130411ab8af0ed6342a5ceabf219", 1483440154, 0x1c307ae4},
        {   1189440, "4745147b3489e4b4729ab742a784c43eb226c35d38aae1bfd25d5e7194ad32ea", 1484346803, 0x1c2685ee},
        {   1209600, "5c878a43a36a69ad0b5dc8fb7e096a539fd0967d72a481312401883ea73feec5", 1485249388, 0x1d00c670},
        {   1229760, "f4e20d3b16c88d23de231b2beaad7bafcc50b30f5d718208c238c71f78beb05b", 1486165012, 0x1c2a44d5},
        {   1249920, "8f71f9d233be160b29e219742f73568d1a8a758ee743fa271a38eb5ac37de446", 1487078910, 0x1c1e6e2b},
        {   1270080, "28019a8a68e778c954c89c5ebbb1991cb08fb0737007ae7dea2495cd0d51178f", 1487995679, 0x1c261acc},
        {   1290240, "6a777a843415c0923ed68badad2403b5d8bbe6d15d8012d3fc1a911d3c5f2bcb", 1488903599, 0x1c2c5a63},
        {   1310400, "6b88659760a7f08e8529c659e2262738afc1596b9e3d07c09dde7526596193e2", 1489809495, 0x1c12a18f},
        {   1330560, "dad9c93aa4c00854d081676be8305f84e31b4834974aa1d94e67b604ef5e02c0", 1490722170, 0x1c1b62dc},
        {   1350720, "c196f3ac806417149b2355830e1e33a209c2ce2f0118931cd5ad17c9285b71de", 1491623405, 0x1c3e9f0e},
        {   1370880, "e82358120235e26f032f2d06ee8cccbc50c7f31b9947e1558124794fe3a7959e", 1492520339, 0x1c3c409b},
        {   1391040, "4afdc077274662bc04173a3d6a4dd4abbb680e60c36b06ca06dc5e9e0b12b1e6", 1493433516, 0x1c2a48ce},
        {   1411200, "3dee0b625fae45748df2150395971359e2bcaaf3bf4dade01fe57c64a8bc5248", 1494347380, 0x1c0562f3},
        {   1431360, "948e41620dddd1baa5c6b1fff2261b143af883f005b33627520150503eb11b02", 1495262244, 0x1d008d9b},
        {   1451520, "cb3ee80464d16f9a2611d716a03463737cef52ec28c558e406c3f90873fa5aa7", 1496175811, 0x1d01ed64},
        {   1471680, "06cd8b6fcfadfa00aa7bfb759213031a8f19920de04364a9268950d04b946e49", 1497123502, 0x1c4dd53c},
        {   1491840, "979617fd86048bc11358eb9edd1af78f878121226d23c387b6d61102fd885ae7", 1499071101, 0x1d00b728},
        {   1512000, "79de6116c1b6892e2d65e9781f89e404618d0b1784a7b53716cb5c4cffd94498", 1506239150, 0x1b4cc5a5},
        {   1532160, "080c11f7a9b4dddf71ab7448dc5d5152aeec9629abd7dcc0060b756c31d17aa0", 1507080147, 0x1c0e26f7},
        {   1552320, "6e90c95478ec65c4f7812b1cf49e0b62c2d663f5f1c35cf2feaa92717c7aa414", 1507971487, 0x1b4acfad},
        {   1572480, "03a5d682bf64882f6f5cef97db0f7cce014c874f6ef33095d5acc6a825f4cf1c", 1508895409, 0x1b6e1c14},
        {   1592640, "e20f3bd2aad7855abca96b978a6697642a517280d2cbd88f259a47fcb3f20f24", 1509822299, 0x1c008d54},
        {   1612800, "8f1b86b2d9d2005627849cc5fe89443cc6ceee318614522395f1598c2ba3f2a7", 1510747283, 0x1b6624dd},
        {   1632960, "5c17ba4d5658b1519b465d469ba0e0b48cf318739f869cd87423f3fcb6075641", 1511675144, 0x1c0166e4},
        {   1653120, "656e87899d2da7dd02c451768c54d477f1c856355e9ce035e992be7ed9f4f845", 1512595932, 0x1c01bc8a},
        {   1673280, "ccd796e545bd39190795d5cf941d0fe85b374ff3ab7a0bc6575747a8ce145ae0", 1513523464, 0x1c0186e4},
        {   1693440, "85eed3adb6daf90876b47477345155d251c795a71d38a564d3ff9ddf9386a10b", 1514515318, 0x1b4b6ded},
        {   1713600, "dd01e8ca7467dc631962169d7d43c9ed8b92216559c3dac3d6ad13db2ff7ab3a", 1515419636, 0x1b4676e9},
        {   1733760, "10534364a48260dcb18ca6f078f8e6572585079ffa0ea0f2f60ca6b3c52b137c", 1516191579, 0x1b129811},
        {   1753920, "bba5b510e2e22c58a461fdb8efac81dbb7245a4b5119714ede2c36d9949ad81a", 1517108999, 0x1b112b7b},
        {   1774080, "b91769f158b77ad81da46632b4b597c91d226cc9a8997075756ebe876d3f3f6c", 1518019159, 0x1b0c755f},
        {   1794240, "8156be9ea67edebccbaa6efd6afc991d485fd5105b2b9ba3744bd2418b2bc8a9", 1518934042, 0x1b15f5f9},
        {   1814400, "5a2b71ca8465396bba4c8201f595758f61b073c6eca535f49464256eecfc0807", 1519831930, 0x1b279095},
        {   1834560, "70928619269698d70776a65acd82662b0d63d49a014402209475cc10fa6f1019", 1520753352, 0x1b1f5c35},
        {   1854720, "f209b481f008e5d24f29f71e23b4fce5015c925c26b92b711be3b81542c822ae", 1521668708, 0x1b36bd25},
        {   1874880, "c83416583cd2f182926186588953e9efab96d67c20928a6220f88e323f033b0f", 1522562773, 0x1b34367c},
        {   1895040, "359d88be27b3a128cb83fae0a76f79d91af4821490d81ed59a169f55656452d8", 1523293934, 0x1c009c65},
        {   1915200, "dcb1db8abeb6525b32a8316ecd498f11f695a772449cde4a2c8099629ba3d9e9", 1524218742, 0x1c00a776},
        {   1935360, "4e0ee3d4912ec852513581fe05c17793ce67a53afdc99ed9df636af4fcc870a6", 1525142283, 0x1c00a770},
        {   1955520, "3613ef61387c7a8a0430997f381d5d1f4a58a55108906fc1339f4012652a44ac", 1527524793, 0x1b017581},
        {   1975680, "a713ded971f6e3d8b2797a305e7e247ecc7d34e90adb045bcfcc749c4e29982f", 1530748587, 0x1b02138f},
        {   1995840, "2f40b51561ec1023f315acc8d2b67e4f465c8af3b14c64887062b6b10bf6bc0d", 1534019870, 0x1b02c46d},
        {   2016000, "553b6e1ff224f5a65bb4cd95ee5020cc7dbefd05e2e407d25fb4028fc6f81b81", 1537221233, 0x1b0248d1},
        {   2036160, "964be0e94ed4427cae3af290478fe66d56f8542e321bcd395911b3c6226f4d72", 1540439719, 0x1b05ad47},
        {   2056320, "ad509815fbfd8add8764367d1a0a563553ca308806294277ed85eea27d918dfe", 1543669468, 0x1b10d8f1},
        {   2076480, "e988fb657410f4536667e310a07ac80ccf5932603285f28c4890a8fbe6f88e36", 1546920752, 0x1b2584cc},
        {   2096640, "d55002744fb19a3360bdaf58c77390da772c6ef384471813b91d051b66d70c59", 1550315436, 0x1b6504f1},
        {   2116800, "d68b55985ec513038db22b7645c98994050e16ad98840b104cb811c2b2753d68", 1553562758, 0x1b3d1950},
};

static const char *dns_seeds[] = {
        "10.0.2.2",
        "5.189.131.197",
        "213.133.98.197",
        "84.104.69.194"
};
#endif

typedef struct {
    BRPeerManager *manager;
    const char *hostname;
    uint64_t services;
} BRFindPeersInfo;

typedef struct {
    BRPeer *peer;
    BRPeerManager *manager;
    UInt256 hash;
} BRPeerCallbackInfo;

typedef struct {
    BRTransaction *tx;
    void *info;

    void (*callback)(void *info, int error);
} BRPublishedTx;

typedef struct {
    UInt256 txHash;
    BRPeer *peers;
} BRTxPeerList;

// true if peer is contained in the list of peers associated with txHash
static int _BRTxPeerListHasPeer(const BRTxPeerList *list, UInt256 txHash, const BRPeer *peer) {
    for (size_t i = array_count(list); i > 0; i--) {
        if (!UInt256Eq(list[i - 1].txHash, txHash)) continue;

        for (size_t j = array_count(list[i - 1].peers); j > 0; j--) {
            if (BRPeerEq(&list[i - 1].peers[j - 1], peer)) return 1;
        }

        break;
    }

    return 0;
}

// number of peers associated with txHash
static size_t _BRTxPeerListCount(const BRTxPeerList *list, UInt256 txHash) {
    for (size_t i = array_count(list); i > 0; i--) {
        if (UInt256Eq(list[i - 1].txHash, txHash)) return array_count(list[i - 1].peers);
    }

    return 0;
}

// adds peer to the list of peers associated with txHash and returns the new total number of peers
static size_t _BRTxPeerListAddPeer(BRTxPeerList **list, UInt256 txHash, const BRPeer *peer) {
    for (size_t i = array_count(*list); i > 0; i--) {
        if (!UInt256Eq((*list)[i - 1].txHash, txHash)) continue;

        for (size_t j = array_count((*list)[i - 1].peers); j > 0; j--) {
            if (BRPeerEq(&(*list)[i - 1].peers[j - 1], peer))
                return array_count((*list)[i - 1].peers);
        }

        array_add((*list)[i - 1].peers, *peer);
        return array_count((*list)[i - 1].peers);
    }

    array_add(*list, ((BRTxPeerList) {txHash, NULL}));
    array_new((*list)[array_count(*list) - 1].peers, PEER_MAX_CONNECTIONS);
    array_add((*list)[array_count(*list) - 1].peers, *peer);
    return 1;
}

// removes peer from the list of peers associated with txHash, returns true if peer was found
static int _BRTxPeerListRemovePeer(BRTxPeerList *list, UInt256 txHash, const BRPeer *peer) {
    for (size_t i = array_count(list); i > 0; i--) {
        if (!UInt256Eq(list[i - 1].txHash, txHash)) continue;

        for (size_t j = array_count(list[i - 1].peers); j > 0; j--) {
            if (!BRPeerEq(&list[i - 1].peers[j - 1], peer)) continue;
            array_rm(list[i - 1].peers, j - 1);
            return 1;
        }

        break;
    }

    return 0;
}

// comparator for sorting peers by timestamp, most recent first
inline static int _peerTimestampCompare(const void *peer, const void *otherPeer) {
    if (((const BRPeer *) peer)->timestamp < ((const BRPeer *) otherPeer)->timestamp) return 1;
    if (((const BRPeer *) peer)->timestamp > ((const BRPeer *) otherPeer)->timestamp) return -1;
    return 0;
}

// returns a hash value for a block's prevBlock value suitable for use in a hashtable
inline static size_t _BRPrevBlockHash(const void *block) {
    return (size_t) ((const BRMerkleBlock *) block)->prevBlock.u32[0];
}

// true if block and otherBlock have equal prevBlock values
inline static int _BRPrevBlockEq(const void *block, const void *otherBlock) {
    return UInt256Eq(((const BRMerkleBlock *) block)->prevBlock,
                     ((const BRMerkleBlock *) otherBlock)->prevBlock);
}

// returns a hash value for a block's height value suitable for use in a hashtable
inline static size_t _BRBlockHeightHash(const void *block) {
    // (FNV_OFFSET xor height)*FNV_PRIME
    return (size_t) ((0x811C9dc5 ^ ((const BRMerkleBlock *) block)->height) * 0x01000193);
}

// true if block and otherBlock have equal height values
inline static int _BRBlockHeightEq(const void *block, const void *otherBlock) {
    return (((const BRMerkleBlock *) block)->height ==
            ((const BRMerkleBlock *) otherBlock)->height);
}

struct BRPeerManagerStruct {
    BRWallet *wallet;
    int isConnected, connectFailureCount, misbehavinCount, dnsThreadCount, maxConnectCount;
    BRPeer *peers, *downloadPeer, fixedPeer, **connectedPeers;
    char downloadPeerName[INET6_ADDRSTRLEN + 6];
    uint32_t earliestKeyTime, syncStartHeight, filterUpdateHeight, estimatedHeight;
    BRBloomFilter *bloomFilter;
    double fpRate, averageTxPerBlock;
    BRSet *blocks, *orphans, *checkpoints;
    BRMerkleBlock *lastBlock, *lastOrphan;
    BRTxPeerList *txRelays, *txRequests;
    BRPublishedTx *publishedTx;
    UInt256 *publishedTxHashes;
    void *info;

    void (*syncStarted)(void *info);

    void (*syncStopped)(void *info, int error);

    void (*txStatusUpdate)(void *info);

    void (*saveBlocks)(void *info, int replace, BRMerkleBlock *blocks[], size_t blocksCount);

    void (*savePeers)(void *info, int replace, const BRPeer peers[], size_t peersCount);

    int (*networkIsReachable)(void *info);

    void (*threadCleanup)(void *info);

    pthread_mutex_t lock;
};

static void _BRPeerManagerPeerMisbehavin(BRPeerManager *manager, BRPeer *peer) {
    for (size_t i = array_count(manager->peers); i > 0; i--) {
        if (BRPeerEq(&manager->peers[i - 1], peer)) array_rm(manager->peers, i - 1);
    }

    if (++manager->misbehavinCount >=
        10) { // clear out stored peers so we get a fresh list from DNS for next connect
        manager->misbehavinCount = 0;
        array_clear(manager->peers);
    }

    BRPeerDisconnect(peer);
}

static void _BRPeerManagerSyncStopped(BRPeerManager *manager) {
    manager->syncStartHeight = 0;

    if (manager->downloadPeer) {
        // don't cancel timeout if there's a pending tx publish callback
        for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
            if (manager->publishedTx[i - 1].callback != NULL) return;
        }

        BRPeerScheduleDisconnect(manager->downloadPeer, -1); // cancel sync timeout
    }
}

// adds transaction to list of tx to be published, along with any unconfirmed inputs
static void _BRPeerManagerAddTxToPublishList(BRPeerManager *manager, BRTransaction *tx, void *info,
                                             void (*callback)(void *, int)) {
    if (tx && tx->blockHeight == TX_UNCONFIRMED) {
        for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
            if (BRTransactionEq(manager->publishedTx[i - 1].tx, tx)) return;
        }

        array_add(manager->publishedTx, ((BRPublishedTx) {tx, info, callback}));
        array_add(manager->publishedTxHashes, tx->txHash);

        for (size_t i = 0; i < tx->inCount; i++) {
            _BRPeerManagerAddTxToPublishList(manager, BRWalletTransactionForHash(manager->wallet,
                                                                                 tx->inputs[i].txHash),
                                             NULL, NULL);
        }
    }
}

static size_t
_BRPeerManagerBlockLocators(BRPeerManager *manager, UInt256 locators[], size_t locatorsCount) {
    // append 10 most recent block hashes, decending, then continue appending, doubling the step back each time,
    // finishing with the genesis block (top, -1, -2, -3, -4, -5, -6, -7, -8, -9, -11, -15, -23, -39, -71, -135, ..., 0)
    BRMerkleBlock *block = manager->lastBlock;
    int32_t step = 1, i = 0, j;

    while (block && block->height > 0) {
        if (locators && i < locatorsCount) locators[i] = block->blockHash;
        if (++i >= 10) step *= 2;

        for (j = 0; block && j < step; j++) {
            block = BRSetGet(manager->blocks, &block->prevBlock);
        }
    }

    if (locators && i < locatorsCount) locators[i] = GENESIS_BLOCK_HASH;
    return ++i;
}

static void _setApplyFreeBlock(void *info, void *block) {
    BRMerkleBlockFree(block);
}

static void _BRPeerManagerLoadBloomFilter(BRPeerManager *manager, BRPeer *peer) {
    // every time a new wallet address is added, the bloom filter has to be rebuilt, and each address is only used
    // for one transaction, so here we generate some spare addresses to avoid rebuilding the filter each time a
    // wallet transaction is encountered during the chain sync
    BRWalletUnusedAddrs(manager->wallet, NULL, SEQUENCE_GAP_LIMIT_EXTERNAL + 100, 0);
    BRWalletUnusedAddrs(manager->wallet, NULL, SEQUENCE_GAP_LIMIT_INTERNAL + 100, 1);

    BRSetApply(manager->orphans, NULL, _setApplyFreeBlock);
    BRSetClear(manager->orphans); // clear out orphans that may have been received on an old filter
    manager->lastOrphan = NULL;
    manager->filterUpdateHeight = manager->lastBlock->height;
    manager->fpRate = BLOOM_REDUCED_FALSEPOSITIVE_RATE;

    size_t addrsCount = BRWalletAllAddrs(manager->wallet, NULL, 0);
    BRAddress *addrs = malloc(addrsCount * sizeof(*addrs));
    size_t utxosCount = BRWalletUTXOs(manager->wallet, NULL, 0);
    BRUTXO *utxos = malloc(utxosCount * sizeof(*utxos));
    uint32_t blockHeight = (manager->lastBlock->height > 100) ? manager->lastBlock->height - 100
                                                              : 0;
    size_t txCount = BRWalletTxUnconfirmedBefore(manager->wallet, NULL, 0, blockHeight);
    BRTransaction **transactions = malloc(txCount * sizeof(*transactions));
    BRBloomFilter *filter;

    assert(addrs != NULL);
    assert(utxos != NULL);
    assert(transactions != NULL);
    addrsCount = BRWalletAllAddrs(manager->wallet, addrs, addrsCount);
    utxosCount = BRWalletUTXOs(manager->wallet, utxos, utxosCount);
    txCount = BRWalletTxUnconfirmedBefore(manager->wallet, transactions, txCount, blockHeight);
    filter = BRBloomFilterNew(manager->fpRate, addrsCount + utxosCount + txCount + 100,
                              (uint32_t) BRPeerHash(peer),
                              BLOOM_UPDATE_ALL); // BUG: XXX txCount not the same as number of spent wallet outputs

    for (size_t i = 0;
         i < addrsCount; i++) { // add addresses to watch for tx receiveing money to the wallet
        UInt160 hash = UINT160_ZERO;

        BRAddressHash160(&hash, addrs[i].s);

        if (!UInt160IsZero(hash) && !BRBloomFilterContainsData(filter, hash.u8, sizeof(hash))) {
            BRBloomFilterInsertData(filter, hash.u8, sizeof(hash));
        }
    }

    free(addrs);

    for (size_t i = 0;
         i < utxosCount; i++) { // add UTXOs to watch for tx sending money from the wallet
        uint8_t o[sizeof(UInt256) + sizeof(uint32_t)];

        UInt256Set(o, utxos[i].hash);
        UInt32SetLE(&o[sizeof(UInt256)], utxos[i].n);
        if (!BRBloomFilterContainsData(filter, o, sizeof(o)))
            BRBloomFilterInsertData(filter, o, sizeof(o));
    }

    free(utxos);

    for (size_t i = 0; i < txCount; i++) { // also add TXOs spent within the last 100 blocks
        for (size_t j = 0; j < transactions[i]->inCount; j++) {
            BRTxInput *input = &transactions[i]->inputs[j];
            BRTransaction *tx = BRWalletTransactionForHash(manager->wallet, input->txHash);
            uint8_t o[sizeof(UInt256) + sizeof(uint32_t)];

            if (tx && input->index < tx->outCount &&
                BRWalletContainsAddress(manager->wallet, tx->outputs[input->index].address)) {
                UInt256Set(o, input->txHash);
                UInt32SetLE(&o[sizeof(UInt256)], input->index);
                if (!BRBloomFilterContainsData(filter, o, sizeof(o)))
                    BRBloomFilterInsertData(filter, o, sizeof(o));
            }
        }
    }

    free(transactions);
    if (manager->bloomFilter) BRBloomFilterFree(manager->bloomFilter);
    manager->bloomFilter = filter;
    // TODO: XXX if already synced, recursively add inputs of unconfirmed receives

    uint8_t data[BRBloomFilterSerialize(filter, NULL, 0)];
    size_t len = BRBloomFilterSerialize(filter, data, sizeof(data));

    BRPeerSendFilterload(peer, data, len);
}

static void _updateFilterRerequestDone(void *info, int success) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;

    free(info);

    if (success) {
        pthread_mutex_lock(&manager->lock);

        if ((peer->flags & PEER_FLAG_NEEDSUPDATE) == 0) {
            UInt256 locators[_BRPeerManagerBlockLocators(manager, NULL, 0)];
            size_t count = _BRPeerManagerBlockLocators(manager, locators,
                                                       sizeof(locators) / sizeof(*locators));

            BRPeerSendGetblocks(peer, locators, count, UINT256_ZERO);
        }

        pthread_mutex_unlock(&manager->lock);
    }
}

static void _updateFilterLoadDone(void *info, int success) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    BRPeerCallbackInfo *peerInfo;

    free(info);

    if (success) {
        pthread_mutex_lock(&manager->lock);
        BRPeerSetNeedsFilterUpdate(peer, 0);
        peer->flags &= ~PEER_FLAG_NEEDSUPDATE;

        if (manager->lastBlock->height < manager->estimatedHeight) { // if syncing, rerequest blocks
            peerInfo = calloc(1, sizeof(*peerInfo));
            assert(peerInfo != NULL);
            peerInfo->peer = peer;
            peerInfo->manager = manager;
            BRPeerRerequestBlocks(manager->downloadPeer, manager->lastBlock->blockHash);
            BRPeerSendPing(manager->downloadPeer, peerInfo, _updateFilterRerequestDone);
        } else BRPeerSendMempool(peer, NULL, 0, NULL, NULL); // if not syncing, request mempool

        pthread_mutex_unlock(&manager->lock);
    }
}

static void _updateFilterPingDone(void *info, int success) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    BRPeerCallbackInfo *peerInfo;

    if (success) {
        pthread_mutex_lock(&manager->lock);
        peer_log(peer, "updating filter with newly created wallet addresses");
        if (manager->bloomFilter) BRBloomFilterFree(manager->bloomFilter);
        manager->bloomFilter = NULL;

        if (manager->lastBlock->height <
            manager->estimatedHeight) { // if we're syncing, only update download peer
            if (manager->downloadPeer) {
                _BRPeerManagerLoadBloomFilter(manager, manager->downloadPeer);
                BRPeerSendPing(manager->downloadPeer, info,
                               _updateFilterLoadDone); // wait for pong so filter is loaded
            } else free(info);
        } else {
            free(info);

            for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
                if (BRPeerConnectStatus(manager->connectedPeers[i - 1]) !=
                    BRPeerStatusConnected)
                    continue;
                peerInfo = calloc(1, sizeof(*peerInfo));
                assert(peerInfo != NULL);
                peerInfo->peer = manager->connectedPeers[i - 1];
                peerInfo->manager = manager;
                _BRPeerManagerLoadBloomFilter(manager, peerInfo->peer);
                BRPeerSendPing(peerInfo->peer, peerInfo,
                               _updateFilterLoadDone); // wait for pong so filter is loaded
            }
        }

        pthread_mutex_unlock(&manager->lock);
    } else free(info);
}

static void _BRPeerManagerUpdateFilter(BRPeerManager *manager) {
    BRPeerCallbackInfo *info;

    if (manager->downloadPeer && (manager->downloadPeer->flags & PEER_FLAG_NEEDSUPDATE) == 0) {
        BRPeerSetNeedsFilterUpdate(manager->downloadPeer, 1);
        manager->downloadPeer->flags |= PEER_FLAG_NEEDSUPDATE;
        peer_log(manager->downloadPeer, "filter update needed, waiting for pong");
        info = calloc(1, sizeof(*info));
        assert(info != NULL);
        info->peer = manager->downloadPeer;
        info->manager = manager;
        // wait for pong so we're sure to include any tx already sent by the peer in the updated filter
        BRPeerSendPing(manager->downloadPeer, info, _updateFilterPingDone);
    }
}

static void _BRPeerManagerUpdateTx(BRPeerManager *manager, const UInt256 txHashes[], size_t txCount,
                                   uint32_t blockHeight, uint32_t timestamp) {
    if (blockHeight != TX_UNCONFIRMED) { // remove confirmed tx from publish list and relay counts
        for (size_t i = 0; i < txCount; i++) {
            for (size_t j = array_count(manager->publishedTx); j > 0; j--) {
                BRTransaction *tx = manager->publishedTx[j - 1].tx;

                if (!UInt256Eq(txHashes[i], tx->txHash)) continue;
                array_rm(manager->publishedTx, j - 1);
                array_rm(manager->publishedTxHashes, j - 1);
                if (!BRWalletTransactionForHash(manager->wallet, tx->txHash)) BRTransactionFree(tx);
            }

            for (size_t j = array_count(manager->txRelays); j > 0; j--) {
                if (!UInt256Eq(txHashes[i], manager->txRelays[j - 1].txHash)) continue;
                array_free(manager->txRelays[j - 1].peers);
                array_rm(manager->txRelays, j - 1);
            }
        }
    }

    BRWalletUpdateTransactions(manager->wallet, txHashes, txCount, blockHeight, timestamp);
}

// unconfirmed transactions that aren't in the mempools of any of connected peers have likely dropped off the network
static void _requestUnrelayedTxGetdataDone(void *info, int success) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    int isPublishing;
    size_t count = 0;

    free(info);
    pthread_mutex_lock(&manager->lock);
    if (success) peer->flags |= PEER_FLAG_SYNCED;

    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        peer = manager->connectedPeers[i - 1];
        if (BRPeerConnectStatus(peer) == BRPeerStatusConnected) count++;
        if ((peer->flags & PEER_FLAG_SYNCED) != 0) continue;
        count = 0;
        break;
    }

    // don't remove transactions until we're connected to maxConnectCount peers, and all peers have finished
    // relaying their mempools
    if (count >= manager->maxConnectCount) {
        size_t txCount = BRWalletTxUnconfirmedBefore(manager->wallet, NULL, 0, TX_UNCONFIRMED);
        BRTransaction *tx[(txCount < 10000) ? txCount : 10000];

        txCount = BRWalletTxUnconfirmedBefore(manager->wallet, tx, sizeof(tx) / sizeof(*tx),
                                              TX_UNCONFIRMED);

        for (size_t i = 0; i < txCount; i++) {
            isPublishing = 0;

            for (size_t j = array_count(manager->publishedTx); !isPublishing && j > 0; j--) {
                if (BRTransactionEq(manager->publishedTx[j - 1].tx, tx[i]) &&
                    manager->publishedTx[j - 1].callback != NULL)
                    isPublishing = 1;
            }

            if (!isPublishing && _BRTxPeerListCount(manager->txRelays, tx[i]->txHash) == 0 &&
                _BRTxPeerListCount(manager->txRequests, tx[i]->txHash) == 0) {
                BRWalletRemoveTransaction(manager->wallet, tx[i]->txHash);
            } else if (!isPublishing && _BRTxPeerListCount(manager->txRelays, tx[i]->txHash) <
                                        manager->maxConnectCount) {
                // set timestamp 0 to mark as unverified
                _BRPeerManagerUpdateTx(manager, &tx[i]->txHash, 1, TX_UNCONFIRMED, 0);
            }
        }
    }

    pthread_mutex_unlock(&manager->lock);
}

static void _BRPeerManagerRequestUnrelayedTx(BRPeerManager *manager, BRPeer *peer) {
    BRPeerCallbackInfo *info;
    size_t hashCount = 0, txCount = BRWalletTxUnconfirmedBefore(manager->wallet, NULL, 0,
                                                                TX_UNCONFIRMED);
    BRTransaction *tx[txCount];
    UInt256 txHashes[txCount];

    txCount = BRWalletTxUnconfirmedBefore(manager->wallet, tx, txCount, TX_UNCONFIRMED);

    for (size_t i = 0; i < txCount; i++) {
        if (!_BRTxPeerListHasPeer(manager->txRelays, tx[i]->txHash, peer) &&
            !_BRTxPeerListHasPeer(manager->txRequests, tx[i]->txHash, peer)) {
            txHashes[hashCount++] = tx[i]->txHash;
            _BRTxPeerListAddPeer(&manager->txRequests, tx[i]->txHash, peer);
        }
    }

    if (hashCount > 0) {
        BRPeerSendGetdata(peer, txHashes, hashCount, NULL, 0);

        if ((peer->flags & PEER_FLAG_SYNCED) == 0) {
            info = calloc(1, sizeof(*info));
            assert(info != NULL);
            info->peer = peer;
            info->manager = manager;
            BRPeerSendPing(peer, info, _requestUnrelayedTxGetdataDone);
        }
    } else peer->flags |= PEER_FLAG_SYNCED;
}

static void _BRPeerManagerPublishPendingTx(BRPeerManager *manager, BRPeer *peer) {
    for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
        if (manager->publishedTx[i - 1].callback == NULL) continue;
        BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // schedule publish timeout
        break;
    }

    BRPeerSendInv(peer, manager->publishedTxHashes, array_count(manager->publishedTxHashes));
}

static void _mempoolDone(void *info, int success) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    int syncFinished = 0;

    free(info);

    if (success) {
        peer_log(peer, "mempool request finished");
        pthread_mutex_lock(&manager->lock);
        if (manager->syncStartHeight > 0) {
            peer_log(peer, "sync succeeded");
            syncFinished = 1;
            _BRPeerManagerSyncStopped(manager);
        }

        _BRPeerManagerRequestUnrelayedTx(manager, peer);
        BRPeerSendGetaddr(peer); // request a list of other bitcoin peers
        pthread_mutex_unlock(&manager->lock);
        if (manager->txStatusUpdate) manager->txStatusUpdate(manager->info);
        if (syncFinished && manager->syncStopped) manager->syncStopped(manager->info, 0);
    } else
        peer_log(peer, "mempool request failed");
}

static void _loadBloomFilterDone(void *info, int success) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;

    pthread_mutex_lock(&manager->lock);

    if (success) {
        BRPeerSendMempool(peer, manager->publishedTxHashes, array_count(manager->publishedTxHashes),
                          info,
                          _mempoolDone);
        pthread_mutex_unlock(&manager->lock);
    } else {
        free(info);

        if (peer == manager->downloadPeer) {
            peer_log(peer, "sync succeeded");
            _BRPeerManagerSyncStopped(manager);
            pthread_mutex_unlock(&manager->lock);
            if (manager->syncStopped) manager->syncStopped(manager->info, 0);
        } else pthread_mutex_unlock(&manager->lock);
    }
}

static void _BRPeerManagerLoadMempools(BRPeerManager *manager) {
    // after syncing, load filters and get mempools from other peers
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        BRPeer *peer = manager->connectedPeers[i - 1];
        BRPeerCallbackInfo *info;

        if (BRPeerConnectStatus(peer) != BRPeerStatusConnected) continue;
        info = calloc(1, sizeof(*info));
        assert(info != NULL);
        info->peer = peer;
        info->manager = manager;

        if (peer != manager->downloadPeer ||
            manager->fpRate > BLOOM_REDUCED_FALSEPOSITIVE_RATE * 5.0) {
            _BRPeerManagerLoadBloomFilter(manager, peer);
            _BRPeerManagerPublishPendingTx(manager, peer);
            BRPeerSendPing(peer, info,
                           _loadBloomFilterDone); // load mempool after updating bloomfilter
        } else
            BRPeerSendMempool(peer, manager->publishedTxHashes,
                              array_count(manager->publishedTxHashes), info,
                              _mempoolDone);
    }
}

// returns a UINT128_ZERO terminated array of addresses for hostname that must be freed, or NULL if lookup failed
static UInt128 *_addressLookup(const char *hostname) {
    struct addrinfo *servinfo, *p;
    UInt128 *addrList = NULL;
    size_t count = 0, i = 0;

    if (getaddrinfo(hostname, NULL, NULL, &servinfo) == 0) {
        for (p = servinfo; p != NULL; p = p->ai_next) count++;
        if (count > 0) addrList = calloc(count + 1, sizeof(*addrList));
        assert(addrList != NULL || count == 0);

        for (p = servinfo; p != NULL; p = p->ai_next) {
            if (p->ai_family == AF_INET) {
                addrList[i].u16[5] = 0xffff;
                addrList[i].u32[3] = ((struct sockaddr_in *) p->ai_addr)->sin_addr.s_addr;
                i++;
            } else if (p->ai_family == AF_INET6) {
                addrList[i++] = *(UInt128 *) &((struct sockaddr_in6 *) p->ai_addr)->sin6_addr;
            }
        }

        freeaddrinfo(servinfo);
    }

    return addrList;
}

static void *_findPeersThreadRoutine(void *arg) {
    BRPeerManager *manager = ((BRFindPeersInfo *) arg)->manager;
    uint64_t services = ((BRFindPeersInfo *) arg)->services;
    UInt128 *addrList, *addr;
    time_t now = time(NULL), age;

    pthread_cleanup_push(manager->threadCleanup, manager->info);
        addrList = _addressLookup(((BRFindPeersInfo *) arg)->hostname);
        free(arg);
        pthread_mutex_lock(&manager->lock);

        for (addr = addrList; addr && !UInt128IsZero(*addr); addr++) {
            age = 24 * 60 * 60 + BRRand(2 * 24 * 60 * 60); // add between 1 and 3 days
            array_add(manager->peers, ((BRPeer) {*addr, STANDARD_PORT, services, now - age, 0}));
        }

        manager->dnsThreadCount--;
        pthread_mutex_unlock(&manager->lock);
        if (addrList) free(addrList);
            pthread_cleanup_pop(1);
    return NULL;
}

// DNS peer discovery
static void _BRPeerManagerFindPeers(BRPeerManager *manager) {
    static const uint64_t services = SERVICES_NODE_NETWORK | SERVICES_NODE_BLOOM;
    time_t now = time(NULL);
    struct timespec ts;
    pthread_t thread;
    pthread_attr_t attr;
    UInt128 *addr, *addrList;
    BRFindPeersInfo *info;

    if (!UInt128IsZero(manager->fixedPeer.address)) {
        array_set_count(manager->peers, 1);
        manager->peers[0] = manager->fixedPeer;
        manager->peers[0].services = services;
        manager->peers[0].timestamp = now;
    } else {
        for (size_t i = 1; i < DNS_SEEDS_COUNT; i++) {
            info = calloc(1, sizeof(BRFindPeersInfo));
            assert(info != NULL);
            info->manager = manager;
            info->hostname = dns_seeds[i];
            info->services = services;
            if (pthread_attr_init(&attr) == 0 &&
                pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) == 0 &&
                pthread_create(&thread, &attr, _findPeersThreadRoutine, info) == 0)
                manager->dnsThreadCount++;
        }

        for (addr = addrList = _addressLookup(dns_seeds[0]);
             addr && !UInt128IsZero(*addr); addr++) {
            array_add(manager->peers, ((BRPeer) {*addr, STANDARD_PORT, services, now, 0}));
        }

        if (addrList) free(addrList);
        ts.tv_sec = 0;
        ts.tv_nsec = 1;

        do {
            pthread_mutex_unlock(&manager->lock);
            nanosleep(&ts, NULL); // pthread_yield() isn't POSIX standard :(
            pthread_mutex_lock(&manager->lock);
        } while (manager->dnsThreadCount > 0 && array_count(manager->peers) < PEER_MAX_CONNECTIONS);

        qsort(manager->peers, array_count(manager->peers), sizeof(*manager->peers),
              _peerTimestampCompare);
    }
}

static void _peerConnected(void *info) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    BRPeerCallbackInfo *peerInfo;
    time_t now = time(NULL);

    pthread_mutex_lock(&manager->lock);
    if (peer->timestamp > now + 2 * 60 * 60 || peer->timestamp < now - 2 * 60 * 60)
        peer->timestamp = now; // sanity check

    // TODO: XXX does this work with 0.11 pruned nodes?
    if (!(peer->services & SERVICES_NODE_NETWORK)) {
        peer_log(peer, "node doesn't carry full blocks");
        BRPeerDisconnect(peer);
    } else if (BRPeerLastBlock(peer) + 10 < manager->lastBlock->height) {
        peer_log(peer, "node isn't synced");
        BRPeerDisconnect(peer);
    } else if ((peer->services & SERVICES_NODE_BCASH) == SERVICES_NODE_BCASH) {
        peer_log(peer, "b-cash nodes not supported");
        BRPeerDisconnect(peer);
    } else if (BRPeerVersion(peer) >= 70011 && !(peer->services & SERVICES_NODE_BLOOM)) {
        peer_log(peer, "node doesn't support SPV mode");
        BRPeerDisconnect(peer);
    } else if (manager->downloadPeer && // check if we should stick with the existing download peer
               (BRPeerLastBlock(manager->downloadPeer) >= BRPeerLastBlock(peer) ||
                manager->lastBlock->height >= BRPeerLastBlock(peer))) {
        if (manager->lastBlock->height >=
            BRPeerLastBlock(peer)) { // only load bloom filter if we're done syncing
            manager->connectFailureCount = 0; // also reset connect failure count if we're already synced
            _BRPeerManagerLoadBloomFilter(manager, peer);
            _BRPeerManagerPublishPendingTx(manager, peer);
            peerInfo = calloc(1, sizeof(*peerInfo));
            assert(peerInfo != NULL);
            peerInfo->peer = peer;
            peerInfo->manager = manager;
            BRPeerSendPing(peer, peerInfo, _loadBloomFilterDone);
        }
    } else { // select the peer with the lowest ping time to download the chain from if we're behind
        // BUG: XXX a malicious peer can report a higher lastblock to make us select them as the download peer, if
        // two peers agree on lastblock, use one of those two instead
        for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
            BRPeer *p = manager->connectedPeers[i - 1];

            if (BRPeerConnectStatus(p) != BRPeerStatusConnected) continue;
            if ((BRPeerPingTime(p) < BRPeerPingTime(peer) &&
                 BRPeerLastBlock(p) >= BRPeerLastBlock(peer)) ||
                BRPeerLastBlock(p) > BRPeerLastBlock(peer))
                peer = p;
        }

        if (manager->downloadPeer) BRPeerDisconnect(manager->downloadPeer);
        manager->downloadPeer = peer;
        manager->isConnected = 1;
        manager->estimatedHeight = BRPeerLastBlock(peer);
        _BRPeerManagerLoadBloomFilter(manager, peer);
        BRPeerSetCurrentBlockHeight(peer, manager->lastBlock->height);
        _BRPeerManagerPublishPendingTx(manager, peer);

        if (manager->lastBlock->height < BRPeerLastBlock(peer)) { // start blockchain sync
            UInt256 locators[_BRPeerManagerBlockLocators(manager, NULL, 0)];
            size_t count = _BRPeerManagerBlockLocators(manager, locators,
                                                       sizeof(locators) / sizeof(*locators));

            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // schedule sync timeout

            // request just block headers up to a week before earliestKeyTime, and then merkleblocks after that
            // we do not reset connect failure count yet incase this request times out
            if (manager->lastBlock->timestamp + 7 * 24 * 60 * 60 >= manager->earliestKeyTime) {
                BRPeerSendGetblocks(peer, locators, count, UINT256_ZERO);
            } else BRPeerSendGetheaders(peer, locators, count, UINT256_ZERO);
        } else { // we're already synced
            manager->connectFailureCount = 0; // reset connect failure count
            _BRPeerManagerLoadMempools(manager);
        }
    }

    pthread_mutex_unlock(&manager->lock);
}

static void _peerDisconnected(void *info, int error) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    BRTxPeerList *peerList;
    int willSave = 0, willReconnect = 0, txError = 0;
    size_t txCount = 0;

    //free(info);
    pthread_mutex_lock(&manager->lock);

    void *txInfo[array_count(manager->publishedTx)];
    void (*txCallback[array_count(manager->publishedTx)])(void *, int);

    if (error == EPROTO) { // if it's protocol error, the peer isn't following standard policy
        _BRPeerManagerPeerMisbehavin(manager, peer);
    } else if (error) { // timeout or some non-protocol related network error
        for (size_t i = array_count(manager->peers); i > 0; i--) {
            if (BRPeerEq(&manager->peers[i - 1], peer)) array_rm(manager->peers, i - 1);
        }

        manager->connectFailureCount++;

        // if it's a timeout and there's pending tx publish callbacks, the tx publish timed out
        // BUG: XXX what if it's a connect timeout and not a publish timeout?
        if (error == ETIMEDOUT && (peer != manager->downloadPeer || manager->syncStartHeight == 0 ||
                                   array_count(manager->connectedPeers) == 1))
            txError = ETIMEDOUT;
    }

    for (size_t i = array_count(manager->txRelays); i > 0; i--) {
        peerList = &manager->txRelays[i - 1];

        for (size_t j = array_count(peerList->peers); j > 0; j--) {
            if (BRPeerEq(&peerList->peers[j - 1], peer)) array_rm(peerList->peers, j - 1);
        }
    }

    if (peer == manager->downloadPeer) { // download peer disconnected
        manager->isConnected = 0;
        manager->downloadPeer = NULL;
        if (manager->connectFailureCount > MAX_CONNECT_FAILURES)
            manager->connectFailureCount = MAX_CONNECT_FAILURES;
    }

    if (!manager->isConnected && manager->connectFailureCount == MAX_CONNECT_FAILURES) {
        _BRPeerManagerSyncStopped(manager);

        // clear out stored peers so we get a fresh list from DNS on next connect attempt
        array_clear(manager->peers);
        txError = ENOTCONN; // trigger any pending tx publish callbacks
        willSave = 1;
        peer_log(peer, "sync failed");
    } else if (manager->connectFailureCount < MAX_CONNECT_FAILURES) willReconnect = 1;

    if (txError) {
        for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
            if (manager->publishedTx[i - 1].callback == NULL) continue;
            peer_log(peer, "transaction canceled: %s", strerror(txError));
            txInfo[txCount] = manager->publishedTx[i - 1].info;
            txCallback[txCount] = manager->publishedTx[i - 1].callback;
            txCount++;
            BRTransactionFree(manager->publishedTx[i - 1].tx);
            array_rm(manager->publishedTxHashes, i - 1);
            array_rm(manager->publishedTx, i - 1);
        }
    }

    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        if (manager->connectedPeers[i - 1] != peer) continue;
        array_rm(manager->connectedPeers, i - 1);
        break;
    }

    BRPeerFree(peer);
    pthread_mutex_unlock(&manager->lock);

    for (size_t i = 0; i < txCount; i++) {
        txCallback[i](txInfo[i], txError);
    }

    if (willSave && manager->savePeers) manager->savePeers(manager->info, 1, NULL, 0);
    if (willSave && manager->syncStopped) manager->syncStopped(manager->info, error);
    if (willReconnect) BRPeerManagerConnect(manager); // try connecting to another peer
    if (manager->txStatusUpdate) manager->txStatusUpdate(manager->info);
}

static void _peerRelayedPeers(void *info, const BRPeer peers[], size_t peersCount) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    time_t now = time(NULL);

    pthread_mutex_lock(&manager->lock);
    peer_log(peer, "relayed %zu peer(s)", peersCount);

    array_add_array(manager->peers, peers, peersCount);
    qsort(manager->peers, array_count(manager->peers), sizeof(*manager->peers),
          _peerTimestampCompare);

    // limit total to 2500 peers
    if (array_count(manager->peers) > 2500) array_set_count(manager->peers, 2500);
    peersCount = array_count(manager->peers);

    // remove peers more than 3 hours old, or until there are only 1000 left
    while (peersCount > 1000 &&
           manager->peers[peersCount - 1].timestamp + 3 * 60 * 60 < now)
        peersCount--;
    array_set_count(manager->peers, peersCount);

    BRPeer save[peersCount];

    for (size_t i = 0; i < peersCount; i++) save[i] = manager->peers[i];
    pthread_mutex_unlock(&manager->lock);

    // peer relaying is complete when we receive <1000
    if (peersCount > 1 && peersCount < 1000 &&
        manager->savePeers)
        manager->savePeers(manager->info, 1, save, peersCount);
}

static void _peerRelayedTx(void *info, BRTransaction *tx) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    void *txInfo = NULL;
    void (*txCallback)(void *, int) = NULL;
    int isWalletTx = 0, hasPendingCallbacks = 0;
    size_t relayCount = 0;

    pthread_mutex_lock(&manager->lock);
    peer_log(peer, "relayed tx: %s", u256_hex_encode(tx->txHash));

    for (size_t i = array_count(manager->publishedTx);
         i > 0; i--) { // see if tx is in list of published tx
        if (UInt256Eq(manager->publishedTxHashes[i - 1], tx->txHash)) {
            txInfo = manager->publishedTx[i - 1].info;
            txCallback = manager->publishedTx[i - 1].callback;
            manager->publishedTx[i - 1].info = NULL;
            manager->publishedTx[i - 1].callback = NULL;
            relayCount = _BRTxPeerListAddPeer(&manager->txRelays, tx->txHash, peer);
        } else if (manager->publishedTx[i - 1].callback != NULL) hasPendingCallbacks = 1;
    }

    // cancel tx publish timeout if no publish callbacks are pending, and syncing is done or this is not downloadPeer
    if (!hasPendingCallbacks && (manager->syncStartHeight == 0 || peer != manager->downloadPeer)) {
        BRPeerScheduleDisconnect(peer, -1); // cancel publish tx timeout
    }

    if (manager->syncStartHeight == 0 || BRWalletContainsTransaction(manager->wallet, tx)) {
        isWalletTx = BRWalletRegisterTransaction(manager->wallet, tx);
        if (isWalletTx) tx = BRWalletTransactionForHash(manager->wallet, tx->txHash);
    } else {
        BRTransactionFree(tx);
        tx = NULL;
    }

    if (tx && isWalletTx) {
        // reschedule sync timeout
        if (manager->syncStartHeight > 0 && peer == manager->downloadPeer) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT);
        }

        if (BRWalletAmountSentByTx(manager->wallet, tx) > 0 &&
            BRWalletTransactionIsValid(manager->wallet, tx)) {
            _BRPeerManagerAddTxToPublishList(manager, tx, NULL,
                                             NULL); // add valid send tx to mempool
        }

        // keep track of how many peers have or relay a tx, this indicates how likely the tx is to confirm
        // (we only need to track this after syncing is complete)
        if (manager->syncStartHeight == 0)
            relayCount = _BRTxPeerListAddPeer(&manager->txRelays, tx->txHash, peer);

        _BRTxPeerListRemovePeer(manager->txRequests, tx->txHash, peer);

        if (manager->bloomFilter != NULL) { // check if bloom filter is already being updated
            BRAddress addrs[SEQUENCE_GAP_LIMIT_EXTERNAL + SEQUENCE_GAP_LIMIT_INTERNAL];
            UInt160 hash;

            // the transaction likely consumed one or more wallet addresses, so check that at least the next <gap limit>
            // unused addresses are still matched by the bloom filter
            BRWalletUnusedAddrs(manager->wallet, addrs, SEQUENCE_GAP_LIMIT_EXTERNAL, 0);
            BRWalletUnusedAddrs(manager->wallet, addrs + SEQUENCE_GAP_LIMIT_EXTERNAL,
                                SEQUENCE_GAP_LIMIT_INTERNAL, 1);

            for (size_t i = 0; i < SEQUENCE_GAP_LIMIT_EXTERNAL + SEQUENCE_GAP_LIMIT_INTERNAL; i++) {
                if (!BRAddressHash160(&hash, addrs[i].s) ||
                    BRBloomFilterContainsData(manager->bloomFilter, hash.u8, sizeof(hash)))
                    continue;
                if (manager->bloomFilter) BRBloomFilterFree(manager->bloomFilter);
                manager->bloomFilter = NULL; // reset bloom filter so it's recreated with new wallet addresses
                _BRPeerManagerUpdateFilter(manager);
                break;
            }
        }
    }

    // set timestamp when tx is verified
    if (tx && relayCount >= manager->maxConnectCount && tx->blockHeight == TX_UNCONFIRMED &&
        tx->timestamp == 0) {
        _BRPeerManagerUpdateTx(manager, &tx->txHash, 1, TX_UNCONFIRMED, (uint32_t) time(NULL));
    }

    pthread_mutex_unlock(&manager->lock);
    if (txCallback) txCallback(txInfo, 0);
}

static void _peerHasTx(void *info, UInt256 txHash) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    BRTransaction *tx;
    void *txInfo = NULL;
    void (*txCallback)(void *, int) = NULL;
    int isWalletTx = 0, hasPendingCallbacks = 0;
    size_t relayCount = 0;

    pthread_mutex_lock(&manager->lock);
    tx = BRWalletTransactionForHash(manager->wallet, txHash);
    peer_log(peer, "has tx: %s", u256_hex_encode(txHash));

    for (size_t i = array_count(manager->publishedTx);
         i > 0; i--) { // see if tx is in list of published tx
        if (UInt256Eq(manager->publishedTxHashes[i - 1], txHash)) {
            if (!tx) tx = manager->publishedTx[i - 1].tx;
            txInfo = manager->publishedTx[i - 1].info;
            txCallback = manager->publishedTx[i - 1].callback;
            manager->publishedTx[i - 1].info = NULL;
            manager->publishedTx[i - 1].callback = NULL;
            relayCount = _BRTxPeerListAddPeer(&manager->txRelays, txHash, peer);
        } else if (manager->publishedTx[i - 1].callback != NULL) hasPendingCallbacks = 1;
    }

    // cancel tx publish timeout if no publish callbacks are pending, and syncing is done or this is not downloadPeer
    if (!hasPendingCallbacks && (manager->syncStartHeight == 0 || peer != manager->downloadPeer)) {
        BRPeerScheduleDisconnect(peer, -1); // cancel publish tx timeout
    }

    if (tx) {
        isWalletTx = BRWalletRegisterTransaction(manager->wallet, tx);
        if (isWalletTx) tx = BRWalletTransactionForHash(manager->wallet, tx->txHash);

        // reschedule sync timeout
        if (manager->syncStartHeight > 0 && peer == manager->downloadPeer && isWalletTx) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT);
        }

        // keep track of how many peers have or relay a tx, this indicates how likely the tx is to confirm
        // (we only need to track this after syncing is complete)
        if (manager->syncStartHeight == 0)
            relayCount = _BRTxPeerListAddPeer(&manager->txRelays, txHash, peer);

        // set timestamp when tx is verified
        if (relayCount >= manager->maxConnectCount && tx && tx->blockHeight == TX_UNCONFIRMED &&
            tx->timestamp == 0) {
            _BRPeerManagerUpdateTx(manager, &txHash, 1, TX_UNCONFIRMED, (uint32_t) time(NULL));
        }

        _BRTxPeerListRemovePeer(manager->txRequests, txHash, peer);
    }

    pthread_mutex_unlock(&manager->lock);
    if (txCallback) txCallback(txInfo, 0);
}

static void _peerRejectedTx(void *info, UInt256 txHash, uint8_t code) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    BRTransaction *tx, *t;

    pthread_mutex_lock(&manager->lock);
    peer_log(peer, "rejected tx: %s", u256_hex_encode(txHash));
    tx = BRWalletTransactionForHash(manager->wallet, txHash);
    _BRTxPeerListRemovePeer(manager->txRequests, txHash, peer);

    if (tx) {
        if (_BRTxPeerListRemovePeer(manager->txRelays, txHash, peer) &&
            tx->blockHeight == TX_UNCONFIRMED) {
            // set timestamp 0 to mark tx as unverified
            _BRPeerManagerUpdateTx(manager, &txHash, 1, TX_UNCONFIRMED, 0);
        }

        // if we get rejected for any reason other than double-spend, the peer is likely misconfigured
        if (code != REJECT_SPENT && BRWalletAmountSentByTx(manager->wallet, tx) > 0) {
            for (size_t i = 0;
                 i < tx->inCount; i++) { // check that all inputs are confirmed before dropping peer
                t = BRWalletTransactionForHash(manager->wallet, tx->inputs[i].txHash);
                if (!t || t->blockHeight != TX_UNCONFIRMED) continue;
                tx = NULL;
                break;
            }

            if (tx) _BRPeerManagerPeerMisbehavin(manager, peer);
        }
    }

    pthread_mutex_unlock(&manager->lock);
    if (manager->txStatusUpdate) manager->txStatusUpdate(manager->info);
}

static int
_BRPeerManagerVerifyBlock(BRPeerManager *manager, BRMerkleBlock *block, BRMerkleBlock *prev,
                          BRPeer *peer) {
    uint32_t transitionTime = 0;
    int r = 1;

    // check if we hit a difficulty transition, and find previous transition time
    if ((block->height % BLOCK_DIFFICULTY_INTERVAL) == 0) {
        BRMerkleBlock *b = block;
        UInt256 prevBlock;

        for (uint32_t i = 0; b && i < BLOCK_DIFFICULTY_INTERVAL; i++) {
            b = BRSetGet(manager->blocks, &b->prevBlock);
        }

        if (!b) {
            peer_log(peer, "missing previous difficulty tansition time, can't verify blockHash: %s",
                     u256_hex_encode(block->blockHash));
            r = 0;
        } else {
            transitionTime = b->timestamp;
            prevBlock = b->prevBlock;
        }

        while (b) { // free up some memory
            b = BRSetGet(manager->blocks, &prevBlock);
            if (b) prevBlock = b->prevBlock;

            if (b && (b->height % BLOCK_DIFFICULTY_INTERVAL) != 0) {
                BRSetRemove(manager->blocks, b);
                BRMerkleBlockFree(b);
            }
        }
    }

    // verify block difficulty
    if (r && !BRMerkleBlockVerifyDifficulty(block, prev, transitionTime)) {
        peer_log(peer, "relayed block with invalid difficulty target %x, blockHash: %s",
                 block->target,
                 u256_hex_encode(block->blockHash));
        r = 0;
    }

    if (r) {
        BRMerkleBlock *checkpoint = BRSetGet(manager->checkpoints, block);

        // verify blockchain checkpoints
        if (checkpoint && !BRMerkleBlockEq(block, checkpoint)) {
            peer_log(peer, "relayed a block that differs from the checkpoint at height %"
                    PRIu32
                    ", blockHash: %s, "
                    "expected: %s", block->height, u256_hex_encode(block->blockHash),
                     u256_hex_encode(checkpoint->blockHash));
            r = 0;
        }
    }

    return r;
}

static void _peerRelayedBlock(void *info, BRMerkleBlock *block) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    size_t txCount = BRMerkleBlockTxHashes(block, NULL, 0);
    UInt256 _txHashes[(sizeof(UInt256) * txCount <= 0x1000) ? txCount : 0],
            *txHashes = (sizeof(UInt256) * txCount <= 0x1000) ? _txHashes : malloc(
            txCount * sizeof(*txHashes));
    size_t i, j, fpCount = 0, saveCount = 0;
    BRMerkleBlock orphan, *b, *b2, *prev, *next = NULL;
    uint32_t txTime = 0;

    assert(txHashes != NULL);
    txCount = BRMerkleBlockTxHashes(block, txHashes, txCount);
    pthread_mutex_lock(&manager->lock);
    prev = BRSetGet(manager->blocks, &block->prevBlock);

    if (prev) {
        txTime = block->timestamp / 2 + prev->timestamp / 2;
        block->height = prev->height + 1;
    }

    // track the observed bloom filter false positive rate using a low pass filter to smooth out variance
    if (peer == manager->downloadPeer && block->totalTx > 0) {
        for (i = 0; i < txCount; i++) { // wallet tx are not false-positives
            if (!BRWalletTransactionForHash(manager->wallet, txHashes[i])) fpCount++;
        }

        // moving average number of tx-per-block
        manager->averageTxPerBlock = manager->averageTxPerBlock * 0.999 + block->totalTx * 0.001;

        // 1% low pass filter, also weights each block by total transactions, compared to the avarage
        manager->fpRate =
                manager->fpRate * (1.0 - 0.01 * block->totalTx / manager->averageTxPerBlock) +
                0.01 * fpCount / manager->averageTxPerBlock;

        // false positive rate sanity check
        if (BRPeerConnectStatus(peer) == BRPeerStatusConnected &&
            manager->fpRate > BLOOM_DEFAULT_FALSEPOSITIVE_RATE * 10.0) {
            peer_log(peer, "bloom filter false positive rate %f too high after %"
                    PRIu32
                    " blocks, disconnecting...",
                     manager->fpRate, manager->lastBlock->height + 1 - manager->filterUpdateHeight);
            BRPeerDisconnect(peer);
        } else if (manager->lastBlock->height + 500 < BRPeerLastBlock(peer) &&
                   manager->fpRate > BLOOM_REDUCED_FALSEPOSITIVE_RATE * 10.0) {
            _BRPeerManagerUpdateFilter(manager); // rebuild bloom filter when it starts to degrade
        }
    }

    // ignore block headers that are newer than one week before earliestKeyTime (it's a header if it has 0 totalTx)
    if (block->totalTx == 0 &&
        block->timestamp + 7 * 24 * 60 * 60 > manager->earliestKeyTime + 2 * 60 * 60) {
        BRMerkleBlockFree(block);
        block = NULL;
    } else if (manager->bloomFilter ==
               NULL) { // ingore potentially incomplete blocks when a filter update is pending
        BRMerkleBlockFree(block);
        block = NULL;

        if (peer == manager->downloadPeer &&
            manager->lastBlock->height < manager->estimatedHeight) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // reschedule sync timeout
            manager->connectFailureCount = 0; // reset failure count once we know our initial request didn't timeout
        }
    } else if (!prev) { // block is an orphan
        peer_log(peer, "relayed orphan block %s, previous %s, last block is %s, height %"
                PRIu32,
                 u256_hex_encode(block->blockHash), u256_hex_encode(block->prevBlock),
                 u256_hex_encode(manager->lastBlock->blockHash), manager->lastBlock->height);

        if (block->timestamp + 7 * 24 * 60 * 60 <
            time(NULL)) { // ignore orphans older than one week ago
            BRMerkleBlockFree(block);
            block = NULL;
        } else {
            // call getblocks, unless we already did with the previous block, or we're still syncing
            if (manager->lastBlock->height >= BRPeerLastBlock(peer) &&
                (!manager->lastOrphan ||
                 !UInt256Eq(manager->lastOrphan->blockHash, block->prevBlock))) {
                UInt256 locators[_BRPeerManagerBlockLocators(manager, NULL, 0)];
                size_t locatorsCount = _BRPeerManagerBlockLocators(manager, locators,
                                                                   sizeof(locators) /
                                                                   sizeof(*locators));

                peer_log(peer, "calling getblocks");
                BRPeerSendGetblocks(peer, locators, locatorsCount, UINT256_ZERO);
            }

            BRSetAdd(manager->orphans,
                     block); // BUG: limit total orphans to avoid memory exhaustion attack
            manager->lastOrphan = block;
        }
    } else if (!_BRPeerManagerVerifyBlock(manager, block, prev, peer)) { // block is invalid
        peer_log(peer, "relayed invalid block");
        BRMerkleBlockFree(block);
        block = NULL;
        _BRPeerManagerPeerMisbehavin(manager, peer);
    } else if (UInt256Eq(block->prevBlock,
                         manager->lastBlock->blockHash)) { // new block extends main chain
        if ((block->height % 500) == 0 || txCount > 0 || block->height >= BRPeerLastBlock(peer)) {
            peer_log(peer, "adding block #%"
                    PRIu32
                    ", false positive rate: %f", block->height, manager->fpRate);
        }

        BRSetAdd(manager->blocks, block);
        manager->lastBlock = block;
        if (txCount > 0) _BRPeerManagerUpdateTx(manager, txHashes, txCount, block->height, txTime);
        if (manager->downloadPeer)
            BRPeerSetCurrentBlockHeight(manager->downloadPeer, block->height);

        if (block->height < manager->estimatedHeight && peer == manager->downloadPeer) {
            BRPeerScheduleDisconnect(peer, PROTOCOL_TIMEOUT); // reschedule sync timeout
            manager->connectFailureCount = 0; // reset failure count once we know our initial request didn't timeout
        }

        if ((block->height % BLOCK_DIFFICULTY_INTERVAL) == 0)
            saveCount = 1; // save transition block immediately

        if (block->height == manager->estimatedHeight) { // chain download is complete
            saveCount = (block->height % BLOCK_DIFFICULTY_INTERVAL) + BLOCK_DIFFICULTY_INTERVAL + 1;
            _BRPeerManagerLoadMempools(manager);
        }
    } else if (BRSetContains(manager->blocks,
                             block)) { // we already have the block (or at least the header)
        if ((block->height % 500) == 0 || txCount > 0 || block->height >= BRPeerLastBlock(peer)) {
            peer_log(peer, "relayed existing block #%"
                    PRIu32, block->height);
        }

        b = manager->lastBlock;
        while (b && b->height > block->height)
            b = BRSetGet(manager->blocks, &b->prevBlock); // is block in main chain?

        if (BRMerkleBlockEq(b,
                            block)) { // if it's not on a fork, set block heights for its transactions
            if (txCount > 0)
                _BRPeerManagerUpdateTx(manager, txHashes, txCount, block->height, txTime);
            if (block->height == manager->lastBlock->height) manager->lastBlock = block;
        }

        b = BRSetAdd(manager->blocks, block);

        if (b != block) {
            if (BRSetGet(manager->orphans, b) == b) BRSetRemove(manager->orphans, b);
            if (manager->lastOrphan == b) manager->lastOrphan = NULL;
            BRMerkleBlockFree(b);
        }
    } else if (manager->lastBlock->height < BRPeerLastBlock(peer) &&
               block->height >
               manager->lastBlock->height + 1) { // special case, new block mined durring rescan
        peer_log(peer, "marking new block #%"
                PRIu32
                " as orphan until rescan completes", block->height);
        BRSetAdd(manager->orphans, block); // mark as orphan til we're caught up
        manager->lastOrphan = block;
    } else if (block->height <= checkpoint_array[CHECKPOINT_COUNT -
                                                 1].height) { // fork is older than last checkpoint
        peer_log(peer, "ignoring block on fork older than most recent checkpoint, block #%"
                PRIu32
                ", hash: %s",
                 block->height, u256_hex_encode(block->blockHash));
        BRMerkleBlockFree(block);
        block = NULL;
    } else { // new block is on a fork
        peer_log(peer, "chain fork reached height %"
                PRIu32, block->height);
        BRSetAdd(manager->blocks, block);

        if (block->height >
            manager->lastBlock->height) { // check if fork is now longer than main chain
            b = block;
            b2 = manager->lastBlock;

            while (b && b2 &&
                   !BRMerkleBlockEq(b, b2)) { // walk back to where the fork joins the main chain
                b = BRSetGet(manager->blocks, &b->prevBlock);
                if (b && b->height < b2->height) b2 = BRSetGet(manager->blocks, &b2->prevBlock);
            }

            peer_log(peer, "reorganizing chain from height %"
                    PRIu32
                    ", new height is %"
                    PRIu32, b->height, block->height);

            BRWalletSetTxUnconfirmedAfter(manager->wallet,
                                          b->height); // mark tx after the join point as unconfirmed

            b = block;

            while (b && b2 &&
                   b->height > b2->height) { // set transaction heights for new main chain
                size_t count = BRMerkleBlockTxHashes(b, NULL, 0);
                uint32_t height = b->height, timestamp = b->timestamp;

                if (count > txCount) {
                    txHashes = (txHashes != _txHashes) ? realloc(txHashes,
                                                                 count * sizeof(*txHashes)) :
                               malloc(count * sizeof(*txHashes));
                    assert(txHashes != NULL);
                    txCount = count;
                }

                count = BRMerkleBlockTxHashes(b, txHashes, count);
                b = BRSetGet(manager->blocks, &b->prevBlock);
                if (b) timestamp = timestamp / 2 + b->timestamp / 2;
                if (count > 0)
                    BRWalletUpdateTransactions(manager->wallet, txHashes, count, height, timestamp);
            }

            manager->lastBlock = block;

            if (block->height == manager->estimatedHeight) { // chain download is complete
                saveCount =
                        (block->height % BLOCK_DIFFICULTY_INTERVAL) + BLOCK_DIFFICULTY_INTERVAL + 1;
                _BRPeerManagerLoadMempools(manager);
            }
        }
    }

    if (txHashes != _txHashes) free(txHashes);

    if (block && block->height != BLOCK_UNKNOWN_HEIGHT) {
        if (block->height > manager->estimatedHeight) manager->estimatedHeight = block->height;

        // check if the next block was received as an orphan
        orphan.prevBlock = block->blockHash;
        next = BRSetRemove(manager->orphans, &orphan);
    }

    BRMerkleBlock *saveBlocks[saveCount];

    for (i = 0, b = block; b && i < saveCount; i++) {
        saveBlocks[i] = b;
        b = BRSetGet(manager->blocks, &b->prevBlock);
    }

    // make sure the set of blocks to be saved starts at a difficulty interval
    j = (i > 0) ? saveBlocks[i - 1]->height % BLOCK_DIFFICULTY_INTERVAL : 0;
    if (j > 0) i -= (i > BLOCK_DIFFICULTY_INTERVAL - j) ? BLOCK_DIFFICULTY_INTERVAL - j : i;
    assert(i == 0 || (saveBlocks[i - 1]->height % BLOCK_DIFFICULTY_INTERVAL) == 0);
    pthread_mutex_unlock(&manager->lock);
    if (i > 0 && manager->saveBlocks)
        manager->saveBlocks(manager->info, (i > 1 ? 1 : 0), saveBlocks, i);

    if (block && block->height != BLOCK_UNKNOWN_HEIGHT && block->height >= BRPeerLastBlock(peer) &&
        manager->txStatusUpdate) {
        manager->txStatusUpdate(
                manager->info); // notify that transaction confirmations may have changed
    }

    if (next) _peerRelayedBlock(info, next);
}

static void _peerDataNotfound(void *info, const UInt256 txHashes[], size_t txCount,
                              const UInt256 blockHashes[], size_t blockCount) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;

    pthread_mutex_lock(&manager->lock);

    for (size_t i = 0; i < txCount; i++) {
        _BRTxPeerListRemovePeer(manager->txRelays, txHashes[i], peer);
        _BRTxPeerListRemovePeer(manager->txRequests, txHashes[i], peer);
    }

    pthread_mutex_unlock(&manager->lock);
}

static void _peerSetFeePerKb(void *info, uint64_t feePerKb) {
    BRPeer *p, *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
    uint64_t maxFeePerKb = 0, secondFeePerKb = 0;

    pthread_mutex_lock(&manager->lock);

    for (size_t i = array_count(manager->connectedPeers);
         i > 0; i--) { // find second highest fee rate
        p = manager->connectedPeers[i - 1];
        if (BRPeerConnectStatus(p) != BRPeerStatusConnected) continue;
        if (BRPeerFeePerKb(p) > maxFeePerKb)
            secondFeePerKb = maxFeePerKb, maxFeePerKb = BRPeerFeePerKb(p);
    }

    if (secondFeePerKb * 3 / 2 > DEFAULT_FEE_PER_KB && secondFeePerKb * 3 / 2 <= MAX_FEE_PER_KB &&
        secondFeePerKb * 3 / 2 > BRWalletFeePerKb(manager->wallet)) {
        peer_log(peer, "increasing feePerKb to %"
                PRIu64
                " based on feefilter messages from peers", secondFeePerKb * 3 / 2);
        BRWalletSetFeePerKb(manager->wallet, secondFeePerKb * 3 / 2);
    }

    pthread_mutex_unlock(&manager->lock);
}

//static void _peerRequestedTxPingDone(void *info, int success)
//{
//    BRPeer *peer = ((BRPeerCallbackInfo *)info)->peer;
//    BRPeerManager *manager = ((BRPeerCallbackInfo *)info)->manager;
//    UInt256 txHash = ((BRPeerCallbackInfo *)info)->hash;
//
//    free(info);
//    pthread_mutex_lock(&manager->lock);
//
//    if (success && ! _BRTxPeerListHasPeer(manager->txRequests, txHash, peer)) {
//        _BRTxPeerListAddPeer(&manager->txRequests, txHash, peer);
//        BRPeerSendGetdata(peer, &txHash, 1, NULL, 0); // check if peer will relay the transaction back
//    }
//    
//    pthread_mutex_unlock(&manager->lock);
//}

static BRTransaction *_peerRequestedTx(void *info, UInt256 txHash) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;
//    BRPeerCallbackInfo *pingInfo;
    BRTransaction *tx = NULL;
    void *txInfo = NULL;
    void (*txCallback)(void *, int) = NULL;
    int hasPendingCallbacks = 0, error = 0;

    pthread_mutex_lock(&manager->lock);

    for (size_t i = array_count(manager->publishedTx); i > 0; i--) {
        if (UInt256Eq(manager->publishedTxHashes[i - 1], txHash)) {
            tx = manager->publishedTx[i - 1].tx;
            txInfo = manager->publishedTx[i - 1].info;
            txCallback = manager->publishedTx[i - 1].callback;
            manager->publishedTx[i - 1].info = NULL;
            manager->publishedTx[i - 1].callback = NULL;

            if (tx && !BRWalletTransactionIsValid(manager->wallet, tx)) {
                error = EINVAL;
                array_rm(manager->publishedTx, i - 1);
                array_rm(manager->publishedTxHashes, i - 1);

                if (!BRWalletTransactionForHash(manager->wallet, txHash)) {
                    BRTransactionFree(tx);
                    tx = NULL;
                }
            }
        } else if (manager->publishedTx[i - 1].callback != NULL) hasPendingCallbacks = 1;
    }

    // cancel tx publish timeout if no publish callbacks are pending, and syncing is done or this is not downloadPeer
    if (!hasPendingCallbacks && (manager->syncStartHeight == 0 || peer != manager->downloadPeer)) {
        BRPeerScheduleDisconnect(peer, -1); // cancel publish tx timeout
    }

    if (tx && !error) {
        _BRTxPeerListAddPeer(&manager->txRelays, txHash, peer);
        BRWalletRegisterTransaction(manager->wallet, tx);
    }

//    pingInfo = calloc(1, sizeof(*pingInfo));
//    assert(pingInfo != NULL);
//    pingInfo->peer = peer;
//    pingInfo->manager = manager;
//    pingInfo->hash = txHash;
//    BRPeerSendPing(peer, pingInfo, _peerRequestedTxPingDone);
    pthread_mutex_unlock(&manager->lock);
    if (txCallback) txCallback(txInfo, error);
    return tx;
}

static int _peerNetworkIsReachable(void *info) {
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;

    return (manager->networkIsReachable) ? manager->networkIsReachable(manager->info) : 1;
}

static void _peerThreadCleanup(void *info) {
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;

    free(info);
    if (manager->threadCleanup) manager->threadCleanup(manager->info);
}

static void _dummyThreadCleanup(void *info) {
}

// returns a newly allocated BRPeerManager struct that must be freed by calling BRPeerManagerFree()
BRPeerManager *BRPeerManagerNew(BRWallet *wallet, uint32_t earliestKeyTime, BRMerkleBlock *blocks[],
                                size_t blocksCount,
                                const BRPeer peers[], size_t peersCount) {
    BRPeerManager *manager = calloc(1, sizeof(*manager));
    BRMerkleBlock orphan, *block = NULL;

    assert(manager != NULL);
    assert(wallet != NULL);
    assert(blocks != NULL || blocksCount == 0);
    assert(peers != NULL || peersCount == 0);
    manager->wallet = wallet;
    manager->earliestKeyTime = earliestKeyTime;
    manager->averageTxPerBlock = 1400;
    manager->maxConnectCount = PEER_MAX_CONNECTIONS;
    array_new(manager->peers, peersCount);
    if (peers) array_add_array(manager->peers, peers, peersCount);
    qsort(manager->peers, array_count(manager->peers), sizeof(*manager->peers),
          _peerTimestampCompare);
    array_new(manager->connectedPeers, PEER_MAX_CONNECTIONS);
    manager->blocks = BRSetNew(BRMerkleBlockHash, BRMerkleBlockEq, blocksCount);
    manager->orphans = BRSetNew(_BRPrevBlockHash, _BRPrevBlockEq,
                                blocksCount); // orphans are indexed by prevBlock
    manager->checkpoints = BRSetNew(_BRBlockHeightHash, _BRBlockHeightEq,
                                    100); // checkpoints are indexed by height

    for (size_t i = 0; i < CHECKPOINT_COUNT; i++) {
        block = BRMerkleBlockNew();
        block->height = checkpoint_array[i].height;
        block->blockHash = UInt256Reverse(u256_hex_decode(checkpoint_array[i].hash));
        block->timestamp = checkpoint_array[i].timestamp;
        block->target = checkpoint_array[i].target;
        BRSetAdd(manager->checkpoints, block);
        BRSetAdd(manager->blocks, block);
        if (i == 0 || block->timestamp + 7 * 24 * 60 * 60 < manager->earliestKeyTime)
            manager->lastBlock = block;
    }

    block = NULL;

    for (size_t i = 0; blocks && i < blocksCount; i++) {
        assert(blocks[i]->height !=
               BLOCK_UNKNOWN_HEIGHT); // height must be saved/restored along with serialized block
        BRSetAdd(manager->orphans, blocks[i]);

        if ((blocks[i]->height % BLOCK_DIFFICULTY_INTERVAL) == 0 &&
            (!block || blocks[i]->height > block->height))
            block = blocks[i]; // find last transition block
    }

    while (block) {
        BRSetAdd(manager->blocks, block);
        manager->lastBlock = block;
        orphan.prevBlock = block->prevBlock;
        BRSetRemove(manager->orphans, &orphan);
        orphan.prevBlock = block->blockHash;
        block = BRSetGet(manager->orphans, &orphan);
    }

    array_new(manager->txRelays, 10);
    array_new(manager->txRequests, 10);
    array_new(manager->publishedTx, 10);
    array_new(manager->publishedTxHashes, 10);
    pthread_mutex_init(&manager->lock, NULL);
    manager->threadCleanup = _dummyThreadCleanup;
    return manager;
}

// not thread-safe, set callbacks once before calling BRPeerManagerConnect()
// info is a void pointer that will be passed along with each callback call
// void syncStarted(void *) - called when blockchain syncing starts
// void syncStopped(void *, int) - called when blockchain syncing stops, error is an errno.h code
// void txStatusUpdate(void *) - called when transaction status may have changed such as when a new block arrives
// void saveBlocks(void *, int, BRMerkleBlock *[], size_t) - called when blocks should be saved to the persistent store
// - if replace is true, remove any previously saved blocks first
// void savePeers(void *, int, const BRPeer[], size_t) - called when peers should be saved to the persistent store
// - if replace is true, remove any previously saved peers first
// int networkIsReachable(void *) - must return true when networking is available, false otherwise
// void threadCleanup(void *) - called before a thread terminates to faciliate any needed cleanup
void BRPeerManagerSetCallbacks(BRPeerManager *manager, void *info,
                               void (*syncStarted)(void *info),
                               void (*syncStopped)(void *info, int error),
                               void (*txStatusUpdate)(void *info),
                               void (*saveBlocks)(void *info, int replace, BRMerkleBlock *blocks[],
                                                  size_t blocksCount),
                               void (*savePeers)(void *info, int replace, const BRPeer peers[],
                                                 size_t peersCount),
                               int (*networkIsReachable)(void *info),
                               void (*threadCleanup)(void *info)) {
    assert(manager != NULL);
    manager->info = info;
    manager->syncStarted = syncStarted;
    manager->syncStopped = syncStopped;
    manager->txStatusUpdate = txStatusUpdate;
    manager->saveBlocks = saveBlocks;
    manager->savePeers = savePeers;
    manager->networkIsReachable = networkIsReachable;
    manager->threadCleanup = (threadCleanup) ? threadCleanup : _dummyThreadCleanup;
}

// specifies a single fixed peer to use when connecting to the bitcoin network
// set address to UINT128_ZERO to revert to default behavior
void BRPeerManagerSetFixedPeer(BRPeerManager *manager, UInt128 address, uint16_t port) {
    assert(manager != NULL);
    BRPeerManagerDisconnect(manager);
    pthread_mutex_lock(&manager->lock);
    manager->maxConnectCount = UInt128IsZero(address) ? PEER_MAX_CONNECTIONS : 1;
    manager->fixedPeer = ((BRPeer) {address, port, 0, 0, 0});
    array_clear(manager->peers);
    pthread_mutex_unlock(&manager->lock);
}

// true if currently connected to at least one peer
int BRPeerManagerIsConnected(BRPeerManager *manager) {
    int isConnected;

    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    isConnected = manager->isConnected;
    pthread_mutex_unlock(&manager->lock);
    return isConnected;
}

// connect to bitcoin peer-to-peer network (also call this whenever networkIsReachable() status changes)
void BRPeerManagerConnect(BRPeerManager *manager) {
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    if (manager->connectFailureCount >= MAX_CONNECT_FAILURES)
        manager->connectFailureCount = 0; //this is a manual retry

    if ((!manager->downloadPeer || manager->lastBlock->height < manager->estimatedHeight) &&
        manager->syncStartHeight == 0) {
        manager->syncStartHeight = manager->lastBlock->height + 1;
        pthread_mutex_unlock(&manager->lock);
        if (manager->syncStarted) manager->syncStarted(manager->info);
        pthread_mutex_lock(&manager->lock);
    }

    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        BRPeer *p = manager->connectedPeers[i - 1];

        if (BRPeerConnectStatus(p) == BRPeerStatusConnecting) BRPeerConnect(p);
    }

    if (array_count(manager->connectedPeers) < manager->maxConnectCount) {
        time_t now = time(NULL);
        BRPeer *peers;

        if (array_count(manager->peers) < manager->maxConnectCount ||
            manager->peers[manager->maxConnectCount - 1].timestamp + 3 * 24 * 60 * 60 < now) {
            _BRPeerManagerFindPeers(manager);
        }

        array_new(peers, 100);
        array_add_array(peers, manager->peers,
                        (array_count(manager->peers) < 100) ? array_count(manager->peers) : 100);

        while (array_count(peers) > 0 &&
               array_count(manager->connectedPeers) < manager->maxConnectCount) {
            size_t i = BRRand((uint32_t) array_count(peers)); // index of random peer
            BRPeerCallbackInfo *info;

            i = i * i / array_count(
                    peers); // bias random peer selection toward peers with more recent timestamp

            for (size_t j = array_count(manager->connectedPeers); i != SIZE_MAX && j > 0; j--) {
                if (!BRPeerEq(&peers[i], manager->connectedPeers[j - 1])) continue;
                array_rm(peers, i); // already in connectedPeers
                i = SIZE_MAX;
            }

            if (i != SIZE_MAX) {
                info = calloc(1, sizeof(*info));
                assert(info != NULL);
                info->manager = manager;
                info->peer = BRPeerNew();
                *info->peer = peers[i];
                array_rm(peers, i);
                array_add(manager->connectedPeers, info->peer);
                BRPeerSetCallbacks(info->peer, info, _peerConnected, _peerDisconnected,
                                   _peerRelayedPeers,
                                   _peerRelayedTx, _peerHasTx, _peerRejectedTx, _peerRelayedBlock,
                                   _peerDataNotfound,
                                   _peerSetFeePerKb, _peerRequestedTx, _peerNetworkIsReachable,
                                   _peerThreadCleanup);
                BRPeerSetEarliestKeyTime(info->peer, manager->earliestKeyTime);
                BRPeerConnect(info->peer);
            }
        }

        array_free(peers);
    }

    if (array_count(manager->connectedPeers) == 0) {
        peer_log(&BR_PEER_NONE, "sync failed");
        _BRPeerManagerSyncStopped(manager);
        pthread_mutex_unlock(&manager->lock);
        if (manager->syncStopped) manager->syncStopped(manager->info, ENETUNREACH);
    } else pthread_mutex_unlock(&manager->lock);
}

void BRPeerManagerDisconnect(BRPeerManager *manager) {
    struct timespec ts;
    size_t peerCount, dnsThreadCount;

    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    peerCount = array_count(manager->connectedPeers);
    dnsThreadCount = manager->dnsThreadCount;

    for (size_t i = peerCount; i > 0; i--) {
        manager->connectFailureCount = MAX_CONNECT_FAILURES; // prevent futher automatic reconnect attempts
        BRPeerDisconnect(manager->connectedPeers[i - 1]);
    }

    pthread_mutex_unlock(&manager->lock);
    ts.tv_sec = 0;
    ts.tv_nsec = 1;

    while (peerCount > 0 || dnsThreadCount > 0) {
        nanosleep(&ts, NULL); // pthread_yield() isn't POSIX standard :(
        pthread_mutex_lock(&manager->lock);
        peerCount = array_count(manager->connectedPeers);
        dnsThreadCount = manager->dnsThreadCount;
        pthread_mutex_unlock(&manager->lock);
    }
}

// rescans blocks and transactions after earliestKeyTime (a new random download peer is also selected due to the
// possibility that a malicious node might lie by omitting transactions that match the bloom filter)
void BRPeerManagerRescan(BRPeerManager *manager) {
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);

    if (manager->isConnected) {
        // start the chain download from the most recent checkpoint that's at least a week older than earliestKeyTime
        for (size_t i = CHECKPOINT_COUNT; i > 0; i--) {
            if (i - 1 == 0 ||
                checkpoint_array[i - 1].timestamp + 7 * 24 * 60 * 60 < manager->earliestKeyTime) {
                UInt256 hash = UInt256Reverse(u256_hex_decode(checkpoint_array[i - 1].hash));

                manager->lastBlock = BRSetGet(manager->blocks, &hash);
                break;
            }
        }

        if (manager->downloadPeer) { // disconnect the current download peer so a new random one will be selected
            for (size_t i = array_count(manager->peers); i > 0; i--) {
                if (BRPeerEq(&manager->peers[i - 1], manager->downloadPeer))
                    array_rm(manager->peers, i - 1);
            }

            BRPeerDisconnect(manager->downloadPeer);
        }

        manager->syncStartHeight = 0; // a syncStartHeight of 0 indicates that syncing hasn't started yet
        pthread_mutex_unlock(&manager->lock);
        BRPeerManagerConnect(manager);
    } else pthread_mutex_unlock(&manager->lock);
}

// the (unverified) best block height reported by connected peers
uint32_t BRPeerManagerEstimatedBlockHeight(BRPeerManager *manager) {
    uint32_t height;

    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    height = (manager->lastBlock->height < manager->estimatedHeight) ? manager->estimatedHeight :
             manager->lastBlock->height;
    pthread_mutex_unlock(&manager->lock);
    return height;
}

// current proof-of-work verified best block height
uint32_t BRPeerManagerLastBlockHeight(BRPeerManager *manager) {
    uint32_t height;

    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    height = manager->lastBlock->height;
    pthread_mutex_unlock(&manager->lock);
    return height;
}

// current proof-of-work verified best block timestamp (time interval since unix epoch)
uint32_t BRPeerManagerLastBlockTimestamp(BRPeerManager *manager) {
    uint32_t timestamp;

    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    timestamp = manager->lastBlock->timestamp;
    pthread_mutex_unlock(&manager->lock);
    return timestamp;
}

// current network sync progress from 0 to 1
// startHeight is the block height of the most recent fully completed sync
double BRPeerManagerSyncProgress(BRPeerManager *manager, uint32_t startHeight) {
    double progress;

    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    if (startHeight == 0) startHeight = manager->syncStartHeight;

    if (!manager->downloadPeer && manager->syncStartHeight == 0) {
        progress = 0.0;
    } else if (!manager->downloadPeer || manager->lastBlock->height < manager->estimatedHeight) {
        if (manager->lastBlock->height > startHeight && manager->estimatedHeight > startHeight) {
            progress = 0.1 + 0.9 * (manager->lastBlock->height - startHeight) /
                             (manager->estimatedHeight - startHeight);
        } else progress = 0.05;
    } else progress = 1.0;

    pthread_mutex_unlock(&manager->lock);
    return progress;
}

// returns the number of currently connected peers
size_t BRPeerManagerPeerCount(BRPeerManager *manager) {
    size_t count = 0;

    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);

    for (size_t i = array_count(manager->connectedPeers); i > 0; i--) {
        if (BRPeerConnectStatus(manager->connectedPeers[i - 1]) == BRPeerStatusConnected) count++;
    }

    pthread_mutex_unlock(&manager->lock);
    return count;
}

// description of the peer most recently used to sync blockchain data
const char *BRPeerManagerDownloadPeerName(BRPeerManager *manager) {
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);

    if (manager->downloadPeer) {
        sprintf(manager->downloadPeerName, "%s:%d", BRPeerHost(manager->downloadPeer),
                manager->downloadPeer->port);
    } else manager->downloadPeerName[0] = '\0';

    pthread_mutex_unlock(&manager->lock);
    return manager->downloadPeerName;
}

static void _publishTxInvDone(void *info, int success) {
    BRPeer *peer = ((BRPeerCallbackInfo *) info)->peer;
    BRPeerManager *manager = ((BRPeerCallbackInfo *) info)->manager;

    free(info);
    pthread_mutex_lock(&manager->lock);
    _BRPeerManagerRequestUnrelayedTx(manager, peer);
    pthread_mutex_unlock(&manager->lock);
}

// publishes tx to bitcoin network (do not call BRTransactionFree() on tx afterward)
void BRPeerManagerPublishTx(BRPeerManager *manager, BRTransaction *tx, void *info,
                            void (*callback)(void *info, int error)) {
    assert(manager != NULL);
    assert(tx != NULL && BRTransactionIsSigned(tx));
    if (tx) pthread_mutex_lock(&manager->lock);

    if (tx && !BRTransactionIsSigned(tx)) {
        pthread_mutex_unlock(&manager->lock);
        BRTransactionFree(tx);
        tx = NULL;
        if (callback) callback(info, EINVAL); // transaction not signed
    } else if (tx && !manager->isConnected) {
        int connectFailureCount = manager->connectFailureCount;

        pthread_mutex_unlock(&manager->lock);

        if (connectFailureCount >= MAX_CONNECT_FAILURES ||
            (manager->networkIsReachable && !manager->networkIsReachable(manager->info))) {
            BRTransactionFree(tx);
            tx = NULL;
            if (callback) callback(info, ENOTCONN); // not connected to bitcoin network
        } else pthread_mutex_lock(&manager->lock);
    }

    if (tx) {
        size_t i, count = 0;

        tx->timestamp = (uint32_t) time(NULL); // set timestamp to publish time
        _BRPeerManagerAddTxToPublishList(manager, tx, info, callback);

        for (i = array_count(manager->connectedPeers); i > 0; i--) {
            if (BRPeerConnectStatus(manager->connectedPeers[i - 1]) == BRPeerStatusConnected)
                count++;
        }

        for (i = array_count(manager->connectedPeers); i > 0; i--) {
            BRPeer *peer = manager->connectedPeers[i - 1];
            BRPeerCallbackInfo *peerInfo;

            if (BRPeerConnectStatus(peer) != BRPeerStatusConnected) continue;

            // instead of publishing to all peers, leave out downloadPeer to see if tx propogates/gets relayed back
            // TODO: XXX connect to a random peer with an empty or fake bloom filter just for publishing
            if (peer != manager->downloadPeer || count == 1) {
                _BRPeerManagerPublishPendingTx(manager, peer);
                peerInfo = calloc(1, sizeof(*peerInfo));
                assert(peerInfo != NULL);
                peerInfo->peer = peer;
                peerInfo->manager = manager;
                BRPeerSendPing(peer, peerInfo, _publishTxInvDone);
            }
        }

        pthread_mutex_unlock(&manager->lock);
    }
}

// number of connected peers that have relayed the given unconfirmed transaction
size_t BRPeerManagerRelayCount(BRPeerManager *manager, UInt256 txHash) {
    size_t count = 0;

    assert(manager != NULL);
    assert(!UInt256IsZero(txHash));
    pthread_mutex_lock(&manager->lock);

    for (size_t i = array_count(manager->txRelays); i > 0; i--) {
        if (!UInt256Eq(manager->txRelays[i - 1].txHash, txHash)) continue;
        count = array_count(manager->txRelays[i - 1].peers);
        break;
    }

    pthread_mutex_unlock(&manager->lock);
    return count;
}

// frees memory allocated for manager
void BRPeerManagerFree(BRPeerManager *manager) {
    assert(manager != NULL);
    pthread_mutex_lock(&manager->lock);
    array_free(manager->peers);
    for (size_t i = array_count(manager->connectedPeers); i > 0; i--)
        BRPeerFree(manager->connectedPeers[i - 1]);
    array_free(manager->connectedPeers);
    BRSetApply(manager->blocks, NULL, _setApplyFreeBlock);
    BRSetFree(manager->blocks);
    BRSetApply(manager->orphans, NULL, _setApplyFreeBlock);
    BRSetFree(manager->orphans);
    BRSetFree(manager->checkpoints);
    for (size_t i = array_count(manager->txRelays); i > 0; i--)
        free(manager->txRelays[i - 1].peers);
    array_free(manager->txRelays);
    for (size_t i = array_count(manager->txRequests); i > 0; i--)
        free(manager->txRequests[i - 1].peers);
    array_free(manager->txRequests);
    array_free(manager->publishedTx);
    array_free(manager->publishedTxHashes);
    pthread_mutex_unlock(&manager->lock);
    pthread_mutex_destroy(&manager->lock);
    free(manager);
}
