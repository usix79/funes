using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading.Tasks;
using Funes.Explorer.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Funes.Explorer {
    public class Startup {
        public Startup(IConfiguration configuration) {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services) {
            services.AddRazorPages();

            IFunesConnection funesConnection = new UnknownConnection();
            
            var fsConnection = Configuration.GetValue<string>("fs");
            if (!string.IsNullOrEmpty(fsConnection)) {
                funesConnection = new FsConnection(fsConnection);
            }
            else {
                var s3Connection = Configuration.GetValue<string>("s3");
                if (!string.IsNullOrEmpty(s3Connection)) {
                    funesConnection = new S3Connection(s3Connection);
                }
            }
            services.AddSingleton(funesConnection);

            IDomainDeserializer deserializer = new DummyDeserializer();
            
            var decoderName = Configuration.GetValue<string>("decoder");
            var splitIdx = decoderName.IndexOf(',');
            var decoderTypeName = decoderName.Substring(0, splitIdx);
            var decoderAssemblyPath = decoderName.Substring(splitIdx + 1).Trim();

            var loadContext = new PluginLoadContext(decoderAssemblyPath);
            var decoderAssembly = loadContext.LoadFromAssemblyName(new AssemblyName(Path.GetFileNameWithoutExtension(decoderAssemblyPath)));

            var decoderType = decoderAssembly.GetType(decoderTypeName, true, true);
            var decoder = Activator.CreateInstance(decoderType);
            deserializer = new DomainDeserializer((Funes.ISerializer)decoder);
            services.AddSingleton(deserializer);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env) {
            if (env.IsDevelopment()) {
                app.UseDeveloperExceptionPage();
            }
            else {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints => { endpoints.MapRazorPages(); });
        }
    }
}