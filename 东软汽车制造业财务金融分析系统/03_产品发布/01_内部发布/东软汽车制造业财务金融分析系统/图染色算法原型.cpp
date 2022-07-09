#pragma GCC optimize("Ofast")
#include <bits/stdc++.h>
#define maxn 1005
using namespace std;
vector<int> g[maxn];
int c[maxn];//染色，不或不可删除 0，待删除 1，确认可以删除 2
bool vis[maxn];
void dfs(int u){
	vis[u]=1;
	for(auto v:g[u]){
		if(vis[v]||c[v]==2) continue;
		if(c[v]==1) dfs(v);
		if(c[v]==0){
			c[u]=0;
			return;
		}
	}
	c[u]=2;
}
int main(){
//	int n=10;
//	g[1].push_back(4);g[1].push_back(6);g[1].push_back(9);
//	g[2].push_back(5);g[2].push_back(6);
//	g[3].push_back(4);g[3].push_back(5);
//	g[4].push_back(7);
//	g[6].push_back(8);
//	g[8].push_back(6);
//	g[9].push_back(1);g[9].push_back(10);
//	g[10].push_back(1);
//	c[1]=1,c[4]=1,c[6]=1,c[7]=1,c[8]=1,c[10]=1;
	for(int i=1;i<=n;i++){
		if(c[i]==1){
			memset(vis,0,sizeof vis);
			dfs(i);
		}
	}
//	for(int i=1;i<=10;i++) printf("%d\n",c[i]);
	return 0;
}
