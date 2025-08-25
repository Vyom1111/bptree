// Disk-based B+ Tree with bulk loading from sorted CSV files
// Usage: ./program <index_file>
// Commands: bulkload <csv>, insert <key> <val8>, get <key>, stats, help, exit

#include <bits/stdc++.h>
using namespace std;

static constexpr uint32_t PAGE_SIZE = 4096;
using PageId = uint64_t;

class Pager {
    string path_;
    fstream fs_;

    inline void ensureClear() {
        if (!fs_.good()) fs_.clear();
    }

public:
    ~Pager(){ close(); }

    void open(const string& path){
        path_ = path;
        fs_.open(path_, ios::in | ios::out | ios::binary);
        if(!fs_.is_open()){
            fs_.clear();
            fs_.open(path_, ios::out | ios::binary);
            if(!fs_.is_open()) throw runtime_error("create file failed");
            fs_.close();
            fs_.clear();
            fs_.open(path_, ios::in | ios::out | ios::binary);
            if(!fs_.is_open()) throw runtime_error("reopen file failed");
        }

        if(fileSize() < PAGE_SIZE){
            vector<char> zero(PAGE_SIZE, 0);
            ensureClear();
            fs_.seekp(0, ios::beg);
            fs_.write(zero.data(), PAGE_SIZE);
            if(!fs_.good()) throw runtime_error("initialize superblock failed");
            fs_.flush();
        }
    }

    void close(){ if(fs_.is_open()) fs_.close(); }

    uint64_t fileSize(){
        ensureClear();
        fs_.seekg(0, ios::end);
        if(!fs_.good()) { fs_.clear(); fs_.seekg(0, ios::end); }
        auto pos = fs_.tellg();
        if(pos < 0) return 0;
        return (uint64_t)pos;
    }

    PageId allocatePage(){
        vector<char> zero(PAGE_SIZE, 0);
        ensureClear();
        fs_.seekp(0, ios::end);
        auto off = fs_.tellp();
        if(off < 0) { fs_.clear(); fs_.seekp(0, ios::end); off = fs_.tellp(); }
        if(off < 0) throw runtime_error("allocatePage: tellp failed");

        fs_.write(zero.data(), PAGE_SIZE);
        if(!fs_.good()) throw runtime_error("allocatePage: write failed");
        fs_.flush();
        return (uint64_t)off / PAGE_SIZE;
    }

    void readPage(PageId pid, void* out){
        ensureClear();
        fs_.seekg((uint64_t)pid * PAGE_SIZE, ios::beg);
        if(!fs_.good()) { fs_.clear(); fs_.seekg((uint64_t)pid * PAGE_SIZE, ios::beg); }
        fs_.read(reinterpret_cast<char*>(out), PAGE_SIZE);
        if(!fs_.good()) throw runtime_error("readPage failed");
    }

    void writePage(PageId pid, const void* in){
        ensureClear();
        fs_.seekp((uint64_t)pid * PAGE_SIZE, ios::beg);
        if(!fs_.good()) { fs_.clear(); fs_.seekp((uint64_t)pid * PAGE_SIZE, ios::beg); }
        fs_.write(reinterpret_cast<const char*>(in), PAGE_SIZE);
        if(!fs_.good()) throw runtime_error("writePage failed");
        fs_.flush();
    }
};

#pragma pack(push,1)
struct SuperBlock {
    uint64_t magic = 0x4250545245453133ULL;
    PageId   root  = 0;
    PageId   free_list_head = 0;
    uint64_t page_count = 1;
    uint8_t  reserved[PAGE_SIZE - 8 - 8 - 8 - 8]{};
};

struct PageHeaderLeaf {
    uint8_t  is_leaf = 1;
    uint16_t count   = 0;
    PageId   parent  = 0;
    PageId   next    = 0;
};

struct PageHeaderInternal {
    uint8_t  is_leaf = 0;
    uint16_t count   = 0;
    PageId   parent  = 0;
    PageId   leftmost= 0;
};

struct LeafEntry { int32_t key; char value[8]; };
struct InternalEntry { int32_t key; PageId right_child; };
#pragma pack(pop)

static constexpr size_t LEAF_HDR_SZ     = sizeof(PageHeaderLeaf);
static constexpr size_t INTERNAL_HDR_SZ = sizeof(PageHeaderInternal);
static constexpr size_t LEAF_ENTRY_SZ   = sizeof(LeafEntry);
static constexpr size_t INT_ENTRY_SZ    = sizeof(InternalEntry);

static constexpr size_t LEAF_CAPACITY     = (PAGE_SIZE - LEAF_HDR_SZ) / LEAF_ENTRY_SZ;
static constexpr size_t INTERNAL_CAPACITY = (PAGE_SIZE - INTERNAL_HDR_SZ) / INT_ENTRY_SZ;

struct LeafPage {
    PageHeaderLeaf hdr;
    LeafEntry      slots[LEAF_CAPACITY]{};
};

struct InternalPage {
    PageHeaderInternal hdr;
    InternalEntry      slots[INTERNAL_CAPACITY]{};
};

static inline string pad8(string s){
    if(s.size() > 8) s.resize(8);
    if(s.size() < 8) s.append(8 - s.size(), '\0');
    return s;
}

static inline int lower_bound_leaf(const LeafPage& leaf, int32_t key){
    int lo=0, hi=leaf.hdr.count;
    while(lo<hi){
        int m=(lo+hi)/2;
        if(leaf.slots[m].key < key) lo=m+1; else hi=m;
    }
    return lo;
}

static inline int child_index(const InternalPage& n, int32_t key){
    int lo=0, hi=n.hdr.count;
    while(lo<hi){
        int m=(lo+hi)/2;
        if(key <= n.slots[m].key) hi=m; else lo=m+1;
    }
    return lo;
}

class BPlusTree {
    Pager pager;
    SuperBlock sb{};

    LeafPage readLeaf(PageId pid){ array<char,PAGE_SIZE> b{}; pager.readPage(pid,b.data()); LeafPage p{}; memcpy(&p,b.data(),sizeof(p)); return p; }
    void     writeLeaf(PageId pid, const LeafPage& p){ array<char,PAGE_SIZE> b{}; memcpy(b.data(),&p,sizeof(p)); pager.writePage(pid,b.data()); }
    InternalPage readInternal(PageId pid){ array<char,PAGE_SIZE> b{}; pager.readPage(pid,b.data()); InternalPage p{}; memcpy(&p,b.data(),sizeof(p)); return p; }
    void         writeInternal(PageId pid, const InternalPage& p){ array<char,PAGE_SIZE> b{}; memcpy(b.data(),&p,sizeof(p)); pager.writePage(pid,b.data()); }

    void readSuper(){ pager.readPage(0,&sb); }
    void writeSuper(){ pager.writePage(0,&sb); }

    PageId newLeaf(PageId parent=0){
        PageId pid = pager.allocatePage();
        LeafPage p{}; p.hdr.is_leaf=1; p.hdr.count=0; p.hdr.parent=parent; p.hdr.next=0;
        writeLeaf(pid,p); sb.page_count++; return pid;
    }
    PageId newInternal(PageId parent=0){
        PageId pid = pager.allocatePage();
        InternalPage p{}; p.hdr.is_leaf=0; p.hdr.count=0; p.hdr.parent=parent; p.hdr.leftmost=0;
        writeInternal(pid,p); sb.page_count++; return pid;
    }

    PageId findLeaf(int32_t key){
        PageId pid = sb.root;
        while(true){
            array<char,PAGE_SIZE> b{}; pager.readPage(pid,b.data());
            uint8_t is_leaf = *reinterpret_cast<uint8_t*>(b.data());
            if(is_leaf) return pid;
            InternalPage n{}; memcpy(&n,b.data(),sizeof(n));
            int idx = child_index(n,key);
            pid = (idx==0) ? n.hdr.leftmost : n.slots[idx-1].right_child;
        }
    }

    void insertIntoParent(PageId left_pid, int32_t sep_key, PageId right_pid){
        array<char,PAGE_SIZE> b{}; pager.readPage(left_pid,b.data());
        uint8_t is_leaf = *reinterpret_cast<uint8_t*>(b.data());
        PageId parent_id = 0;
        if(is_leaf){ LeafPage L{}; memcpy(&L,b.data(),sizeof(L)); parent_id=L.hdr.parent; }
        else { InternalPage L{}; memcpy(&L,b.data(),sizeof(L)); parent_id=L.hdr.parent; }

        if(parent_id==0 && sb.root==left_pid){
            PageId root_id = newInternal(0);
            InternalPage r = readInternal(root_id);
            r.hdr.leftmost = left_pid;
            r.slots[0] = {sep_key, right_pid};
            r.hdr.count = 1;
            writeInternal(root_id,r);
            if(is_leaf){ auto L=readLeaf(left_pid); L.hdr.parent=root_id; writeLeaf(left_pid,L); }
            else { auto L=readInternal(left_pid); L.hdr.parent=root_id; writeInternal(left_pid,L); }
            { array<char,PAGE_SIZE> rb{}; pager.readPage(right_pid,rb.data());
              uint8_t leaf=*reinterpret_cast<uint8_t*>(rb.data());
              if(leaf){ auto R=readLeaf(right_pid); R.hdr.parent=root_id; writeLeaf(right_pid,R);}
              else { auto R=readInternal(right_pid); R.hdr.parent=root_id; writeInternal(right_pid,R);}
            }
            sb.root=root_id; writeSuper(); return;
        }

        InternalPage parent = readInternal(parent_id);
        int pos=0; while(pos<parent.hdr.count && parent.slots[pos].key<sep_key) pos++;

        if(parent.hdr.count == INTERNAL_CAPACITY){
            vector<InternalEntry> tmp(parent.hdr.count+1);
            for(int i=0;i<pos;i++) tmp[i]=parent.slots[i];
            tmp[pos] = {sep_key,right_pid};
            for(int i=pos;i<parent.hdr.count;i++) tmp[i+1]=parent.slots[i];
            int total=(int)tmp.size();
            int leftCount = total/2;
            int32_t promoteKey = tmp[leftCount].key;
            InternalPage leftP = parent; leftP.hdr.count = leftCount;
            for(int i=0;i<leftCount;i++) leftP.slots[i]=tmp[i];
            for(int i=leftCount;i<(int)INTERNAL_CAPACITY;i++) leftP.slots[i]={0,0};
            PageId rightNodeId = newInternal(readInternal(parent_id).hdr.parent);
            InternalPage rightP = readInternal(rightNodeId);
            rightP.hdr.leftmost = tmp[leftCount].right_child;
            int rightCount = total - leftCount - 1;
            for(int i=0;i<rightCount;i++) rightP.slots[i]=tmp[leftCount+1+i];
            rightP.hdr.count = rightCount;

            writeInternal(parent_id,leftP);
            writeInternal(rightNodeId,rightP);

            auto fixp = [&](PageId c){
                array<char,PAGE_SIZE> xb{}; pager.readPage(c,xb.data());
                uint8_t lf=*reinterpret_cast<uint8_t*>(xb.data());
                if(lf){ auto p=readLeaf(c); p.hdr.parent=rightNodeId; writeLeaf(c,p); }
                else { auto p=readInternal(c); p.hdr.parent=rightNodeId; writeInternal(c,p); }
            };
            fixp(rightP.hdr.leftmost);
            for(int i=0;i<rightP.hdr.count;i++) fixp(rightP.slots[i].right_child);

            insertIntoParent(parent_id,promoteKey,rightNodeId);
            return;
        }

        for(int i=parent.hdr.count;i>pos;--i) parent.slots[i]=parent.slots[i-1];
        parent.slots[pos]={sep_key,right_pid};
        parent.hdr.count++;
        writeInternal(parent_id,parent);

        array<char,PAGE_SIZE> rb{}; pager.readPage(right_pid,rb.data());
        if(*reinterpret_cast<uint8_t*>(rb.data())){ auto R=readLeaf(right_pid); R.hdr.parent=parent_id; writeLeaf(right_pid,R); }
        else { auto R=readInternal(right_pid); R.hdr.parent=parent_id; writeInternal(right_pid,R); }
    }

    void splitLeafAndInsert(PageId leaf_id, int32_t key, const string& v8){
        LeafPage leaf = readLeaf(leaf_id);
        vector<LeafEntry> tmp(leaf.hdr.count+1);
        int pos = lower_bound_leaf(leaf,key);
        for(int i=0;i<pos;i++) tmp[i]=leaf.slots[i];
        tmp[pos]={key,{0}}; memcpy(tmp[pos].value,v8.data(),8);
        for(int i=pos;i<leaf.hdr.count;i++) tmp[i+1]=leaf.slots[i];

        int total=(int)tmp.size();
        int leftCount = total/2;
        int rightCount= total-leftCount;

        for(int i=0;i<leftCount;i++) leaf.slots[i]=tmp[i];
        for(int i=leftCount;i<(int)LEAF_CAPACITY;i++) leaf.slots[i]={0,{0}};
        leaf.hdr.count=leftCount;

        PageId right_id = newLeaf(leaf.hdr.parent);
        LeafPage right = readLeaf(right_id);
        for(int i=0;i<rightCount;i++) right.slots[i]=tmp[leftCount+i];
        right.hdr.count=rightCount;

        right.hdr.next = leaf.hdr.next;
        leaf.hdr.next  = right_id;

        writeLeaf(leaf_id,leaf);
        writeLeaf(right_id,right);

        int32_t sep_key = right.slots[0].key;
        insertIntoParent(leaf_id,sep_key,right_id);
    }

public:
    void open(const string& path){
        pager.open(path);
        readSuper();
        if(sb.magic != 0x4250545245453133ULL){
            sb = SuperBlock{};
            PageId rootLeaf = newLeaf(0);
            sb.root = rootLeaf;
            writeSuper();
        }
    }

    bool get(int32_t key, string& outVal){
        PageId leaf_id = findLeaf(key);
        LeafPage leaf = readLeaf(leaf_id);
        int pos = lower_bound_leaf(leaf,key);
        if(pos<leaf.hdr.count && leaf.slots[pos].key==key){
            outVal.assign(leaf.slots[pos].value, leaf.slots[pos].value+8);
            return true;
        }
        return false;
    }

    void insert(int32_t key, string val){
        string v8 = pad8(val);
        PageId leaf_id = findLeaf(key);
        LeafPage leaf = readLeaf(leaf_id);
        int pos = lower_bound_leaf(leaf,key);
        if(pos<leaf.hdr.count && leaf.slots[pos].key==key){
            memcpy(leaf.slots[pos].value,v8.data(),8);
            writeLeaf(leaf_id,leaf);
            return;
        }
        if(leaf.hdr.count < LEAF_CAPACITY){
            for(int i=leaf.hdr.count;i>pos;--i) leaf.slots[i]=leaf.slots[i-1];
            leaf.slots[pos]={key,{0}}; memcpy(leaf.slots[pos].value,v8.data(),8);
            leaf.hdr.count++;
            writeLeaf(leaf_id,leaf);
        }else{
            splitLeafAndInsert(leaf_id,key,v8);
        }
    }

    void stats(){
        int height=0; PageId pid=sb.root;
        while(pid){
            array<char,PAGE_SIZE> b{}; pager.readPage(pid,b.data());
            uint8_t is_leaf=*reinterpret_cast<uint8_t*>(b.data());
            height++;
            if(is_leaf) break;
            auto n = readInternal(pid); pid = n.hdr.leftmost;
        }
    }

    void bulkLoadCSV(const string& csvPath){
        ifstream in(csvPath);
        if(!in.is_open()) throw runtime_error("Could not open CSV: " + csvPath);

        vector<pair<int32_t, PageId>> child_list;
        LeafPage leaf{}; int leafFill=0;
        PageId prevLeafPid = 0;

        auto flush_leaf = [&](int32_t firstKey)->PageId {
            PageId pid = pager.allocatePage();
            writeLeaf(pid, leaf);

            if(prevLeafPid){
                LeafPage prev = readLeaf(prevLeafPid);
                prev.hdr.next = pid;
                writeLeaf(prevLeafPid, prev);
            }
            prevLeafPid = pid;

            child_list.emplace_back(firstKey, pid);

            leaf = LeafPage{};
            leafFill = 0;
            return pid;
        };

        auto parse_csv_line = [](const string& line, int32_t& keyOut, string& valOut)->bool{
            if(line.empty()) return false;
            size_t comma = line.find(',');
            if(comma == string::npos) return false;
            string k = line.substr(0, comma);
            string v = line.substr(comma+1);
            auto trim = [](string& s){
                size_t a = s.find_first_not_of(" \t\r\n");
                size_t b = s.find_last_not_of(" \t\r\n");
                if(a==string::npos){ s.clear(); return; }
                s = s.substr(a, b-a+1);
            };
            trim(k); trim(v);
            try{
                long long tmp = stoll(k);
                if(tmp < INT_MIN || tmp > INT_MAX) return false;
                keyOut = (int32_t)tmp;
            }catch(...){ return false; }
            valOut = v;
            return true;
        };

        string line;
        streampos startPos = in.tellg();
        if(std::getline(in,line)){
            int32_t ktmp; string vtmp;
            if(!parse_csv_line(line,ktmp,vtmp)){
            }else{
                in.clear();
                in.seekg(startPos);
            }
        }

        int64_t processed=0;
        bool firstKeySet=false;
        int32_t prevKey = INT32_MIN;

        while(std::getline(in,line)){
            int32_t key; string val;
            if(!parse_csv_line(line,key,val)){
                continue;
            }
            if(firstKeySet && key < prevKey){
                throw runtime_error("bulkLoadCSV: input not sorted by key. Sort the CSV first.");
            }
            firstKeySet=true; prevKey = key;

            leaf.slots[leafFill].key = key;
            {
                string v8 = pad8(val);
                memcpy(leaf.slots[leafFill].value, v8.data(), 8);
            }
            leafFill++; leaf.hdr.count++;

            if(leafFill == (int)LEAF_CAPACITY){
                int32_t firstKey = leaf.slots[0].key;
                flush_leaf(firstKey);
            }

            processed++;
        }
        if(leaf.hdr.count > 0){
            int32_t firstKey = leaf.slots[0].key;
            flush_leaf(firstKey);
        }

        if(child_list.empty()){
            PageId rootLeaf = newLeaf(0);
            sb.root = rootLeaf;
            writeSuper();
            return;
        }

        auto build_level = [&](vector<pair<int32_t,PageId>>& lower)->vector<pair<int32_t,PageId>>{
            vector<pair<int32_t,PageId>> upper;
            size_t i = 0;
            while(i < lower.size()){
                PageId nodeId = pager.allocatePage();
                InternalPage node{};
                node.hdr.leftmost = lower[i].second;
                int cnt = 0;

                size_t j = i + 1;
                while(cnt < (int)INTERNAL_CAPACITY && j < lower.size()){
                    int32_t sep = lower[j].first;
                    PageId  rc  = lower[j].second;
                    node.slots[cnt++] = {sep, rc};
                    ++j;
                }
                node.hdr.count = (uint16_t)cnt;

                auto set_parent = [&](PageId child){
                    array<char,PAGE_SIZE> b{}; pager.readPage(child,b.data());
                    uint8_t is_leaf = *reinterpret_cast<uint8_t*>(b.data());
                    if(is_leaf){ auto p=readLeaf(child); p.hdr.parent=nodeId; writeLeaf(child,p); }
                    else { auto p=readInternal(child); p.hdr.parent=nodeId; writeInternal(child,p); }
                };
                set_parent(node.hdr.leftmost);
                for(int k=0;k<cnt;k++) set_parent(node.slots[k].right_child);

                writeInternal(nodeId,node);

                int32_t promote_key = (cnt>0) ? node.slots[0].key : lower[i].first;
                upper.emplace_back(promote_key, nodeId);

                i = j;
            }
            return upper;
        };

        vector<pair<int32_t,PageId>> level = child_list;
        while(level.size() > 1){
            level = build_level(level);
        }

        sb.root = level[0].second;
        writeSuper();
    }
};

int main(int argc, char* argv[]){
    if(argc < 2){
        return 1;
    }
    string indexFile = argv[1];

    BPlusTree tree;
    try{
        tree.open(indexFile);
    }catch(const exception& e){
        return 1;
    }

    string line;
    while(getline(cin,line)){
        istringstream ss(line);
        string cmd; ss >> cmd;
        if(cmd.empty()){ continue; }

        try{
            if(cmd=="stats"){
                tree.stats();
            }else if(cmd=="get"){
                long long kll;
                if(!(ss >> kll)){ }
                else{
                    if(kll < INT_MIN || kll > INT_MAX){ }
                    else{
                        string v;
                        if(tree.get((int32_t)kll, v)){
                            for(char c: v) cout << (c?c:' ');
                            cout << "\n";
                        }
                    }
                }
            }else if(cmd=="insert"){
                long long kll; string val;
                if(!(ss >> kll >> val)){ }
                else if(kll < INT_MIN || kll > INT_MAX){ }
                else{
                    tree.insert((int32_t)kll, val);
                }
            }else if(cmd=="bulkload"){
                string path;
                if(!(ss >> path)){ }
                else{
                    tree.bulkLoadCSV(path);
                }
            }else if(cmd=="exit" || cmd=="quit"){
                break;
            }
        }catch(const exception& e){
        }
    }
    return 0;
}